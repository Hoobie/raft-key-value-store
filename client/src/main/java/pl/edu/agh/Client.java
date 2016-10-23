package pl.edu.agh;

import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import rx.Observable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;

public class Client {

    public static void main(String[] args) {
        SocketAddress serverAddress = new InetSocketAddress(12345);

        TcpClient.newClient(serverAddress)
                .enableWireLogging("client", LogLevel.DEBUG)
                .createConnectionRequest()
                .flatMap(connection ->
                        connection.writeString(Observable.just("Hello World!"))
                                .cast(ByteBuf.class)
                                .concatWith(connection.getInput())
                )
                .take(1)
                .map(bb -> bb.toString(Charset.defaultCharset()))
                .toBlocking()
                .forEach(System.out::println);
    }
}
