package pl.edu.agh;

import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.agh.messages.DummyMessage;
import pl.edu.agh.utils.MessageUtils;
import rx.Observable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class RaftClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient.class);

    public static void main(String[] args) {
        SocketAddress serverAddress = new InetSocketAddress(12345);

        TcpClient.newClient(serverAddress).enableWireLogging("client", LogLevel.DEBUG)
                .createConnectionRequest()
                .flatMap(connection -> {
                    byte[] msg = SerializationUtils.serialize(new DummyMessage());
                    return connection.writeBytesAndFlushOnEach(Observable.just(msg))
                            .cast(ByteBuf.class)
                            .concatWith(connection.getInput());
                })
                .take(1)
                .toBlocking()
                .forEach(o -> LOGGER.info("Message received: {}", MessageUtils.toObject(o).toString()));
    }
}
