package pl.edu.agh;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.agh.messages.client_communication.*;
import pl.edu.agh.utils.MessageUtils;
import pl.edu.agh.utils.SocketAddressUtils;
import rx.Observable;

import java.nio.charset.Charset;
import java.util.List;

import static javaslang.API.*;
import static javaslang.Predicates.instanceOf;

public class RaftClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient.class);

    private static List<Connection<ByteBuf, ByteBuf>> serverConnections = Lists.newArrayList();

    private ClientCallback callback = null;

    public static void main(String[] args) {
        if (args.length < 1)
            throw new RuntimeException("You have to provide at least one server address and port number!");

        List<Pair<String, Integer>> serverAddresses = Lists.newArrayList();
        for (String arg : args)
            serverAddresses.add(SocketAddressUtils.splitHostAndPort(arg));
        new RaftClient(serverAddresses);
    }

    public RaftClient(List<Pair<String, Integer>> serverAddresses) {
        Connection<ByteBuf, ByteBuf> connection;
        for (Pair<String, Integer> address : serverAddresses) {
            try {
                connection = TcpClient.newClient(address.getLeft(), address.getRight())
                        .enableWireLogging("client", LogLevel.DEBUG)
                        .createConnectionRequest()
                        .toBlocking()
                        .first();
                connection.getInput().forEach(byteBuf -> {
                    handleResponse(MessageUtils.toObject(byteBuf.toString(Charset.defaultCharset())));
                });

                serverConnections.add(connection);
            } catch (Exception ingored) {
                // No all server may be up
            }
        }

        // For now this is a test ;) Will write jUnit test tomorrow ;)
        try {
            setValue("test", 1);
            Thread.sleep(2000);
            getValue("test");
            Thread.sleep(2000);
            removeValue("test");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while (true);
    }

    public void setCallback(ClientCallback callback) {
        this.callback = callback;
    }

    private void handleResponse(Object obj) {
        Match(obj).of(
                Case(instanceOf(GetValueResponse.class), gv -> {
                    LOGGER.info("Get Value response {} ", gv.getValue());
                    if (callback != null) callback.onValueGet(gv.getValue());
                    return null;
                }),
                Case(instanceOf(SetValueResponse.class), sv -> {
                    LOGGER.info("Set Value response {}", sv.isSuccessful());
                    if (callback != null) callback.onValueSet(sv.isSuccessful());
                    return null;
                }),
                Case(instanceOf(RemoveValueResponse.class), rv -> {
                    LOGGER.info("Remove Value response {} ", rv.isSuccessful());
                    if (callback != null) callback.onValueRemoved(rv.isSuccessful());
                    return null;
                }),
                Case($(), o -> {
                    throw new IllegalArgumentException("Wrong message");
                })
        );

    }

    private void getValue(String key) {
        String msg = MessageUtils.toString(new GetValue(key));
        serverConnections.forEach(c -> c.writeString(Observable.just(msg))
                .take(1)
                .toBlocking()
                .forEach(v -> LOGGER.info("Get value request sent for key {} ", key)));
    }

    private void setValue(String key, int value) {
        String msg = MessageUtils.toString(new SetValue(key, value));
        serverConnections.forEach(c -> c.writeString(Observable.just(msg))
                .take(1)
                .toBlocking()
                .forEach(v -> LOGGER.info("Set value request sent for key {} with value {} ", key, value)));
    }

    private void removeValue(String key) {
        String msg = MessageUtils.toString(new RemoveValue(key));
        serverConnections.forEach(c -> c.writeString(Observable.just(msg))
                .take(1)
                .toBlocking()
                .forEach(v -> LOGGER.info("Remove value request sent for key {}", key)));
    }
}
