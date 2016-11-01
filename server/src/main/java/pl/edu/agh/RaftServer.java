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
import pl.edu.agh.messages.DummyMessage;
import pl.edu.agh.messages.Message;
import pl.edu.agh.messages.RequestVote;
import pl.edu.agh.messages.VoteResponse;
import pl.edu.agh.util.MessageUtil;
import pl.edu.agh.util.SocketAddressUtil;
import rx.Observable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static javaslang.API.*;
import static javaslang.Predicates.instanceOf;
import static pl.edu.agh.util.ThreadUtil.sleep;

public class RaftServer {

    private static final int MIN_TIMEOUT_MILLIS = 150;
    private static final int MAX_TIMEOUT_MILLIS = 300;

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftServer.class);

    private static final Random RAND = new Random();
    private static final ScheduledExecutorService TIMEOUT_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    private final SocketAddress localAddress;
    private final TcpServer<ByteBuf, ByteBuf> tcpServer;
    private final Map<SocketAddress, Connection<ByteBuf, ByteBuf>> serverConnections;

    private State state = State.FOLLOWER;
    private int currentTerm = 0;
    private AtomicInteger votesCount = new AtomicInteger(0);
    private SocketAddress votedFor;
    private ScheduledFuture electionTimeout;

    public static void main(final String[] args) {
        Pair<String, Integer> localAddress = SocketAddressUtil.splitHostAndPort(args[0]);
        RaftServer server = new RaftServer(localAddress.getLeft(), localAddress.getRight(),
                ArrayUtils.subarray(args, 1, args.length));
        server.awaitShutdown();
    }

    public RaftServer(String host, int port, String... serversHostsAndPorts) {
        tcpServer = createTcpServer(port);
        this.localAddress = new InetSocketAddress(host, port);

        serverConnections = Arrays.stream(serversHostsAndPorts)
                .map(SocketAddressUtil::splitHostAndPort)
                .map(hostAndPort -> {
                    Connection<ByteBuf, ByteBuf> tcpConnection = createTcpConnection(hostAndPort.getLeft(), hostAndPort.getRight());
                    return Pair.of(tcpConnection.getChannelPipeline().channel().remoteAddress(), tcpConnection);
                })
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

        electionTimeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateTimeout(), TimeUnit.MILLISECONDS);
    }

    private TcpServer<ByteBuf, ByteBuf> createTcpServer(int port) {
        TcpServer<ByteBuf, ByteBuf> tcpServer = TcpServer.newServer(port);
        tcpServer.enableWireLogging("server", LogLevel.DEBUG)
                .start(connection -> connection.writeBytesAndFlushOnEach(connection.getInput()
                        .doOnNext(msg -> LOGGER.info("Received a message"))
                        .map(MessageUtil::toObject)
                        .map(this::handleRequest)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .map(SerializationUtils::serialize)
                ));
        return tcpServer;
    }

    private Optional<Message> handleRequest(Object obj) {
        return Match(obj).of(
                Case(instanceOf(RequestVote.class), (RequestVote rv) -> {
                    boolean granted = false;
                    if (votedFor == null) {
                        votedFor = rv.candidateAddress;
                        granted = true;
                    }
                    VoteResponse response = new VoteResponse(granted);
                    return Optional.of(response);
                }),
                Case(instanceOf(DummyMessage.class), Optional::of),
                Case($(), o -> Optional.empty()));
    }

    private Connection<ByteBuf, ByteBuf> createTcpConnection(String address, int port) {
        try {
            Connection<ByteBuf, ByteBuf> connection = TcpClient.newClient(address, port)
                    .enableWireLogging("server-connection", LogLevel.DEBUG)
                    .createConnectionRequest()
                    .toBlocking()
                    .first();

            connection.getInput().forEach(byteBuf -> {
                byte[] bytes = new byte[byteBuf.readableBytes()];
                byteBuf.getBytes(0, bytes);
                handleResponse(SerializationUtils.deserialize(bytes));
            });

            return connection;
        } catch (Exception ignored) {
            // FIXME: Maybe use retry and delay instead of this
            sleep(1000L);
            return createTcpConnection(address, port);
        }
    }

    private void handleResponse(Object obj) {
        Match(obj).of(Case(instanceOf(VoteResponse.class), (VoteResponse vr) -> {
            LOGGER.info("Received VoteResponse");
            if (vr.granted && votesCount.incrementAndGet() > serverConnections.size() / 2) {
                LOGGER.info("Server {} became a leader", localAddress.toString());
                state = State.LEADER;
                votedFor = null;
                votesCount = new AtomicInteger(0);
                electionTimeout.cancel(false);
                // TODO: send heartbeat
            }
            return null;
        }), Case($(), o -> {
            throw new IllegalArgumentException("Wrong message");
        }));
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
        serverConnections.forEach((remoteAddress, connection) -> {
            votedFor = localAddress;
            RequestVote requestVote = new RequestVote(currentTerm, localAddress);
            connection.writeBytes(Observable.just(SerializationUtils.serialize(requestVote)))
                    .subscribe(o -> LOGGER.info("RequestVote sent"));
        });
    }

    private int calculateTimeout() {
        return MIN_TIMEOUT_MILLIS + RAND.nextInt(MAX_TIMEOUT_MILLIS - MIN_TIMEOUT_MILLIS);
    }
}
