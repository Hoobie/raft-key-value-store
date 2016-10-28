package pl.edu.agh;

import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.protocol.tcp.server.TcpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public class Server {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    public static void main(final String[] args) {
        createServer(Integer.parseInt(args[0])).awaitShutdown();
    }

    private static TcpServer<ByteBuf, ByteBuf> createServer(int port) {
        return TcpServer.newServer(port)
                .enableWireLogging("server", LogLevel.DEBUG)
                .start(connection -> connection
                        .writeStringAndFlushOnEach(connection.getInput()
                                .map(bb -> bb.toString(Charset.defaultCharset()))
                                .doOnNext(LOGGER::info)
                                .map(msg -> "echo => " + msg)));
    }
}
