package pl.edu.agh;

import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import io.reactivex.netty.protocol.tcp.server.TcpServer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.agh.messages.RequestVote;
import pl.edu.agh.messages.VoteResponse;
import rx.Observable;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static pl.edu.agh.MessageMatcher.match;
import static pl.edu.agh.MessageMatcher.message;
import static pl.edu.agh.util.ThreadUtil.sleep;

public class RaftServer {

    private static final int MIN_TIMEOUT_MILLIS = 150;
    private static final int MAX_TIMEOUT_MILLIS = 300;

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftServer.class);

    private static final Random RAND = new Random();
    private static final ScheduledExecutorService TIMEOUT_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    private final String address;
    private final int port;
    private final TcpServer<ByteBuf, ByteBuf> tcpServer;
    private final Map<String, Connection> serverConnections;

    private State state = State.FOLLOWER;
    private int currentTerm = 0;
    private AtomicInteger votesCount = new AtomicInteger(1);
    private String votedFor;
    private ScheduledFuture electionTimeout;

    public static void main(final String[] args) {
        RaftServer server = new RaftServer(Integer.parseInt(args[0]), ArrayUtils.subarray(args, 1, args.length));
        server.awaitShutdown();
    }

    public RaftServer(int port, String... serverAddressesAndPorts) {
        tcpServer = createTcpServer(port);
        this.port = port;
        this.address = tcpServer.getServerAddress().toString();

        serverConnections = Arrays.asList(serverAddressesAndPorts).stream()
                .map(addressAndPort -> addressAndPort.split(":"))
                .map(split -> Pair.of(split[0], createTcpConnection(split[0], Integer.parseInt(split[1]))))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        electionTimeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateTimeout(), TimeUnit.MILLISECONDS);
    }

    private TcpServer<ByteBuf, ByteBuf> createTcpServer(int port) {
        TcpServer<ByteBuf, ByteBuf> tcpServer = TcpServer.newServer(port);
        tcpServer.enableWireLogging("server", LogLevel.DEBUG)
                .start(connection -> connection.getInput()
                        .doOnNext(msg -> LOGGER.info("Received a message"))
                        .map(byteBuf -> {
                            byte[] bytes = new byte[byteBuf.readableBytes()];
                            byteBuf.getBytes(0, bytes);
                            return SerializationUtils.deserialize(bytes);
                        })
                        .map(o -> match(o, message(RequestVote.class, rv -> {
                            boolean granted = false;
                            if (votedFor == null) {
                                votedFor = rv.candidateAddress;
                                granted = true;
                            }
                            VoteResponse response = new VoteResponse(granted);
                            return serverConnections.get(rv.candidateAddress)
                                    .writeBytes(Observable.just(SerializationUtils.serialize(response)));
                        }), message(VoteResponse.class, vr -> {
                            if (vr.granted && votesCount.incrementAndGet() > serverConnections.size() / 2) {
                                LOGGER.info("Server %s:%d became a leader", address, port);
                                state = State.LEADER;
                                votesCount = new AtomicInteger(1);
                                electionTimeout.cancel(false);
                                // TODO: send heartbeat
                            }
                            return Observable.empty();
                        })/* TODO: implement more handlers */))
                );
        return tcpServer;
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
        LOGGER.debug("Timed out");

        switch (state) {
            case FOLLOWER:
                state = State.CANDIDATE;
                LOGGER.info("Starting election");
                startElection();
                break;
            case CANDIDATE:
                LOGGER.info("Restarting election");
                startElection();
                break;
            case LEADER:
                // TODO: implement
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void startElection() {
        currentTerm++;
        electionTimeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateTimeout(), TimeUnit.MILLISECONDS);
        serverConnections.forEach((address, connection) -> {
            RequestVote requestVote = new RequestVote(currentTerm, address + ":" + port);
            connection.writeBytes(Observable.just(SerializationUtils.serialize(requestVote)))
                    .forEach(s -> LOGGER.info("RequestVote sent"));
        });
    }

    private int calculateTimeout() {
        return MIN_TIMEOUT_MILLIS + RAND.nextInt(MAX_TIMEOUT_MILLIS - MIN_TIMEOUT_MILLIS);
    }
}
