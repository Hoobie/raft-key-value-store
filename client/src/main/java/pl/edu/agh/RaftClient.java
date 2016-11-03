package pl.edu.agh;

import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.agh.messages.client_communication.*;
import rx.Observable;

import static javaslang.API.*;
import static javaslang.Predicates.instanceOf;

public class RaftClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient.class);

    private static Connection<ByteBuf, ByteBuf> serverConnection;

    public static void main(String[] args) {
        if (args.length < 1) throw new RuntimeException("You have to provide server port number !");

        serverConnection = TcpClient.newClient("127.0.0.1", Integer.parseInt(args[0]))
                .enableWireLogging("client", LogLevel.DEBUG)
                .createConnectionRequest()
                .toBlocking()
                .first();

        serverConnection.getInput().forEach(byteBuf -> {
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(0, bytes);
            handleResponse(SerializationUtils.deserialize(bytes));
        });

        setValue("test", 1);
        getValue("test");
        removeValue("test");
        while (true);
    }

    private static void handleResponse(Object obj) {
        Match(obj).of(
                Case(instanceOf(GetValueResponse.class), gv -> {
                    LOGGER.info("Get Value response {} ", gv.getValue());
                    return null;
                }),
                Case(instanceOf(SetValueResponse.class), sv -> {
                    LOGGER.info("Set Value response {}", sv.isSuccessful());
                    return null;
                }),
                Case(instanceOf(RemoveValueResponse.class), rv -> {
                    LOGGER.info("Remove Value response {} ", rv.isSuccessful());
                    return null;
                }),
                Case($(), o -> {
                    throw new IllegalArgumentException("Wrong message");
                })
        );

    }

    public static void getValue(String key) {
        byte[] msg = SerializationUtils.serialize(new GetValue(key));
        serverConnection.writeBytes(Observable.just(msg))
                .take(1)
                .toBlocking()
                .forEach(v -> LOGGER.info("Get value request sent for key {} ", key));
    }

    public static void setValue(String key, int value) {
        byte[] msg = SerializationUtils.serialize(new SetValue(key, value));
        serverConnection.writeBytes(Observable.just(msg))
                .take(1)
                .toBlocking()
                .forEach(v -> LOGGER.info("Set value request sent for key {} with value {} ", key, value));
    }

    public static void removeValue(String key) {
        byte[] msg = SerializationUtils.serialize(new RemoveValue(key));
        serverConnection.writeBytes(Observable.just(msg))
                .take(1)
                .toBlocking()
                .forEach(v -> LOGGER.info("Remove value request sent for key {}", key));
    }
}
