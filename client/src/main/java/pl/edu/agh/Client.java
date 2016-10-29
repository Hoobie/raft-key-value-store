package pl.edu.agh;

import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;

public class Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {
        SocketAddress serverAddress = new InetSocketAddress(12345);

        TcpClient.newClient(serverAddress)
                .enableWireLogging("client", LogLevel.DEBUG)
                .createConnectionRequest()
                .flatMap(connection -> connection.writeString(Observable.just("Hello World!")).cast(ByteBuf.class))
                .map(byteBuf -> byteBuf.toString(Charset.defaultCharset()))
                .toBlocking()
                .forEach(LOGGER::info);
    }
}
