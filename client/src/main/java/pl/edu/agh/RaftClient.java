package pl.edu.agh;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.agh.logs.KeyValueStoreAction;
import pl.edu.agh.messages.client.*;
import pl.edu.agh.utils.MessageUtils;
import pl.edu.agh.utils.SocketAddressUtils;
import pl.edu.agh.utils.ThreadUtils;
import rx.Observable;

import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.List;

import static javaslang.API.*;
import static javaslang.Predicates.instanceOf;

public class RaftClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient.class);

    private static List<Connection<ByteBuf, ByteBuf>> serverConnections = Lists.newArrayList();

    private ClientCallback callback = null;

    public static void main(String[] args) {
        if (args.length < 3)
            throw new RuntimeException("Usage: [ACTION] [KEY] [VALUE] [SERVER_ADDRESSES]\n" +
                    "Where ACTION = [SET/GET/REMOVE]\n" +
                    "Where KEY is the key of element in map\n" +
                    "Where VALUE is the new value (only if you're using SET action!)\n" +
                    "Where SERVER_ADDRESSES is a list of server addresses in format address:port\n");

        KeyValueStoreAction action = KeyValueStoreAction.valueOf(args[0].toUpperCase());
        String key = args[1];
        int value = (action == KeyValueStoreAction.SET) ? Integer.parseInt(args[2]) : 0;
        int startIdx = (action == KeyValueStoreAction.SET) ? 3 : 2;
        String[] serverAddresses = ArrayUtils.subarray(args, startIdx, args.length);
        new RaftClient(action, key, value, serverAddresses);
    }

    public RaftClient(KeyValueStoreAction action, String key, int value, String... serverAddresses) {
        Connection<ByteBuf, ByteBuf> connection;
        String[] connectionAddress = new String[1];
        Pair<String, Integer> address;
        for (String addressString : serverAddresses) {
            try {
                address = SocketAddressUtils.splitHostAndPort(addressString);
                connection = TcpClient.newClient(address.getLeft(), address.getRight())
                        .enableWireLogging("client", LogLevel.DEBUG)
                        .createConnectionRequest()
                        .toBlocking()
                        .first();
                connectionAddress[0] = connection.getChannelPipeline().channel().remoteAddress().toString();
                connection.getInput().forEach(byteBuf -> {
                    handleResponse(connectionAddress[0], MessageUtils.toObject(byteBuf.toString(Charset.defaultCharset())));
                });

                serverConnections.add(connection);
            } catch (Exception ingored) {
                // No all server may be up
            }
        }

        switch (action) {
            case SET:
                setValue(key, value);
                break;
            case REMOVE:
                removeValue(key);
                break;
            case GET:
                getValue(key);
                break;
        }

        ThreadUtils.sleep(5000);
    }

    public void setCallback(ClientCallback callback) {
        this.callback = callback;
    }

    private void handleResponse(String connection, Object obj) {
        Match(obj).of(
                Case(instanceOf(GetValueResponse.class), gv -> {
                    LOGGER.info("Get Value response {} from {} ", gv.getValue(), connection);
                    if (callback != null) callback.onValueGet(gv.getValue());
                    return null;
                }),
                Case(instanceOf(SetValueResponse.class), sv -> {
                    LOGGER.info("Set Value response {} from {}", sv.isSuccessful(), connection);
                    if (callback != null) callback.onValueSet(sv.isSuccessful());
                    return null;
                }),
                Case(instanceOf(RemoveValueResponse.class), rv -> {
                    LOGGER.info("Remove Value response {} from {}", rv.isSuccessful(), connection);
                    if (callback != null) callback.onValueRemoved(rv.isSuccessful());
                    return null;
                }),
                Case(instanceOf(KeyNotInStoreResponse.class), kn -> {
                    LOGGER.error("Key {} is not in store response from {}", kn.getKey(), connection);
                    if (callback != null) callback.onKeyNotInStore(kn.getKey());
                    return null;
                }),
                Case($(), o -> {
                    throw new IllegalArgumentException("Wrong message");
                })
        );

    }

    public void getValue(String key) {
        String msg = MessageUtils.toString(new GetValue(key));
        serverConnections.forEach(c -> c.writeString(Observable.just(msg))
                .take(1)
                .toBlocking()
                .forEach(v -> LOGGER.info("Get value request sent for key {} ", key)));
    }

    public void setValue(String key, int value) {
        String msg = MessageUtils.toString(new SetValue(key, value));
        serverConnections.forEach(c -> c.writeString(Observable.just(msg))
                .take(1)
                .toBlocking()
                .forEach(v -> LOGGER.info("Set value request sent for key {} with value {} ", key, value)));
    }

    public void removeValue(String key) {
        String msg = MessageUtils.toString(new RemoveValue(key));
        serverConnections.forEach(c -> c.writeString(Observable.just(msg))
                .take(1)
                .toBlocking()
                .forEach(v -> LOGGER.info("Remove value request sent for key {}", key)));
    }
}
