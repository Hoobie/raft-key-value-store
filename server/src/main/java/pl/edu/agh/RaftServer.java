package pl.edu.agh;

import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import io.reactivex.netty.protocol.tcp.server.TcpServer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static pl.edu.agh.util.ThreadUtil.sleep;

public class RaftServer {

    private static final int MIN_TIMEOUT_MILLIS = 150;
    private static final int MAX_TIMEOUT_MILLIS = 300;

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftServer.class);

    private static final Random RAND = new Random();
    private static final ScheduledExecutorService TIMEOUT_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    private final TcpServer<ByteBuf, ByteBuf> tcpServer;
    private final Map<String, Connection> serverConnections;

    private ScheduledFuture electionTimeout;

    public static void main(final String[] args) {
        RaftServer server = new RaftServer(Integer.parseInt(args[0]), ArrayUtils.subarray(args, 1, args.length));
        server.awaitShutdown();
    }

    public RaftServer(int port, String... serverAddressesAndPorts) {
        tcpServer = createTcpServer(port);

        serverConnections = Arrays.asList(serverAddressesAndPorts).stream()
                .map(addressAndPort -> addressAndPort.split(":"))
                .map(split -> Pair.of(split[0], createTcpConnection(split[0], Integer.parseInt(split[1]))))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        electionTimeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout,
                MIN_TIMEOUT_MILLIS + RAND.nextInt(MAX_TIMEOUT_MILLIS - MIN_TIMEOUT_MILLIS), TimeUnit.MILLISECONDS);
    }

    private TcpServer<ByteBuf, ByteBuf> createTcpServer(int port) {
        return TcpServer.newServer(port)
                .enableWireLogging("server", LogLevel.DEBUG)
                .start(connection -> connection.writeStringAndFlushOnEach(connection.getInput()
                        .map(byteBuf -> byteBuf.toString(Charset.defaultCharset()))
                        .doOnNext(msg -> LOGGER.info("Received: " + msg))
                        .map(msg -> "echo -> " + msg)));
    }

    private Connection<ByteBuf, ByteBuf> createTcpConnection(String address, int port) {
        try {
            return TcpClient.newClient(address, port)
                    .enableWireLogging("server-connection", LogLevel.DEBUG)
                    .createConnectionRequest()
                    .toBlocking()
                    .first();
        } catch (Exception ignored) {
            // FIXME: Maybe use retry and delay instead of this
            sleep(1000L);
            return createTcpConnection(address, port);
        }
    }

    private void awaitShutdown() {
        tcpServer.awaitShutdown();
    }

    private void handleTimeout() {
        // TODO: check state and perform proper action
        LOGGER.info("Timed out");

        serverConnections.forEach((address, connection) -> connection.writeString(Observable.just("Hello World!"))
                .toBlocking()
                .first());
    }
}
