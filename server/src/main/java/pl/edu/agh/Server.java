package pl.edu.agh;

import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.protocol.tcp.server.TcpServer;

import java.nio.charset.Charset;

public class Server {

    public static void main(final String[] args) {
        TcpServer<ByteBuf, ByteBuf> server;

        server = TcpServer.newServer(12345)
                .enableWireLogging("server", LogLevel.DEBUG)
                .start(connection -> connection
                        .writeStringAndFlushOnEach(connection.getInput()
                                .map(bb -> bb.toString(Charset.defaultCharset()))
                                .doOnNext(System.out::println)
                                .map(msg -> "echo => " + msg)));

        server.awaitShutdown();
    }
}
