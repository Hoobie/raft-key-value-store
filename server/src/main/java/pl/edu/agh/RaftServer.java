package pl.edu.agh;

import com.google.common.collect.Maps;
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
import pl.edu.agh.messages.RaftMessage;
import pl.edu.agh.messages.client_communication.*;
import pl.edu.agh.messages.election.RequestVote;
import pl.edu.agh.messages.election.VoteResponse;
import pl.edu.agh.messages.replication.AppendEntries;
import pl.edu.agh.messages.replication.AppendEntriesResponse;
import pl.edu.agh.utils.LogArchive;
import pl.edu.agh.utils.MessageUtils;
import pl.edu.agh.utils.SocketAddressUtils;
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
import static pl.edu.agh.utils.ThreadUtils.sleep;

public class RaftServer {

    private static final int MIN_ELECTION_TIMEOUT_MILLIS = 150;
    private static final int MAX_ELECTION_TIMEOUT_MILLIS = 300;
    private static final int HEARTBEAT_TIMEOUT_MILLIS = 100;

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
    private ScheduledFuture timeout;

    private Map<String, Integer> keyValueStore = Maps.newHashMap();
    private LogArchive logArchive;

    public static void main(final String[] args) {
        Pair<String, Integer> localAddress = SocketAddressUtils.splitHostAndPort(args[0]);
        new RaftServer(localAddress.getLeft(), localAddress.getRight(),
                ArrayUtils.subarray(args, 1, args.length));
    }

    public RaftServer(String host, int port, String... serversHostsAndPorts) {
        logArchive = new LogArchive();
        tcpServer = createTcpServer(port);
        this.localAddress = new InetSocketAddress(host, port);

        serverConnections = Arrays.stream(serversHostsAndPorts)
                .map(SocketAddressUtils::splitHostAndPort)
                .map(hostAndPort -> {
                    Connection<ByteBuf, ByteBuf> tcpConnection = createTcpConnection(hostAndPort.getLeft(), hostAndPort.getRight());
                    return Pair.of(tcpConnection.getChannelPipeline().channel().remoteAddress(), tcpConnection);
                })
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

        timeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateElectionTimeout(), TimeUnit.MILLISECONDS);
    }

    public State getState() {
        return state;
    }

    private TcpServer<ByteBuf, ByteBuf> createTcpServer(int port) {
        TcpServer<ByteBuf, ByteBuf> tcpServer = TcpServer.newServer(port);
        tcpServer.enableWireLogging("server", LogLevel.DEBUG)
                .start(connection -> connection.writeBytesAndFlushOnEach(connection.getInput()
                        .map(MessageUtils::toObject)
                        .map(this::handleRequest)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .map(SerializationUtils::serialize)
                ));
        return tcpServer;
    }

    private Optional<RaftMessage> handleRequest(Object obj) {
        return Match(obj).of(
                Case(instanceOf(RequestVote.class), rv -> {
                    boolean granted = false;
                    if (votedFor == null || true /*FIXME: check term and being up-to-date*/) {
                        votedFor = rv.candidateAddress;
                        granted = true;
                    }
                    VoteResponse response = new VoteResponse(granted);
                    return Optional.of(response);
                }),
                Case(instanceOf(AppendEntries.class), ae -> {
                    // TODO: check content and perform proper actions
                    // for now it is only a heartbeat
                    AppendEntriesResponse response = new AppendEntriesResponse();
                    if (ae.term >= currentTerm) {
                        LOGGER.info("The leader have spoken");
                        state = State.FOLLOWER;
                        timeout.cancel(false);
                        timeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateElectionTimeout(), TimeUnit.MILLISECONDS);
                    }
                    return Optional.of(response);
                }),
                Case(instanceOf(DummyMessage.class), Optional::of),
                Case(instanceOf(ClientMessage.class), cm -> {
                    if (state == State.LEADER)
                        return handleClientMessage(cm);
                    return Optional.empty();
                }),
                Case($(), o -> Optional.empty())
        );
    }

    private Optional<RaftMessage> handleClientMessage(ClientMessage cm) {
        LOGGER.info("I'm a leader and I got this client message: " + cm.toString());

        return Match(cm).of(
                Case(instanceOf(GetValue.class), gv -> {
                    GetValueResponse response = new GetValueResponse(keyValueStore.get(gv.getKey()));
                    return Optional.of(response);
                }),
                Case(instanceOf(SetValue.class), sv -> {
                    keyValueStore.put(sv.getKey(), sv.getValue());
                    SetValueResponse response = new SetValueResponse(true);
                    return Optional.of(response);
                }),
                Case(instanceOf(RemoveValue.class), rv -> {
                    keyValueStore.remove(rv.getKey());
                    RemoveValueResponse response = new RemoveValueResponse(true);
                    return Optional.of(response);
                }),
                Case($(), o -> Optional.empty())
        );
    }

    private void resendClientMessage(ClientMessage cm) {
        serverConnections.forEach((remoteAddress, connection) -> {
            connection.writeBytes(Observable.just(SerializationUtils.serialize(cm)))
                    .take(1)
                    .toBlocking()
                    .forEach(v -> LOGGER.info("Client message resent"));
        });
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
        Match(obj).of(
                Case(instanceOf(VoteResponse.class), vr -> {
                    LOGGER.info("Received VoteResponse");
                    if (vr.granted && isMajority(votesCount.incrementAndGet())) {
                        LOGGER.info("Server {} became a leader", localAddress.toString());
                        state = State.LEADER;
                        votedFor = null;
                        votesCount = new AtomicInteger(0);
                        timeout.cancel(false);
                        timeout = TIMEOUT_EXECUTOR.scheduleAtFixedRate(this::handleTimeout, 0,
                                HEARTBEAT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                    }
                    return null;
                }),
                Case(instanceOf(AppendEntriesResponse.class), aer -> {
                    LOGGER.info("AppendEntriesResponse received");
                    return null;
                }),
                Case($(), o -> {
                    throw new IllegalArgumentException("Wrong message");
                })
        );
    }

    private boolean isMajority(int votesCount) {
        return votesCount > serverConnections.size() / 2;
    }

    private void handleTimeout() {
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
                sendHeartbeat();
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void startElection() {
        currentTerm++;
        timeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateElectionTimeout(), TimeUnit.MILLISECONDS);
        serverConnections.forEach((remoteAddress, connection) -> {
            votedFor = localAddress;
            RequestVote requestVote = new RequestVote(currentTerm, localAddress);
            connection.writeBytes(Observable.just(SerializationUtils.serialize(requestVote)))
                    .take(1)
                    .toBlocking()
                    .forEach(v -> LOGGER.info("RequestVote sent"));
        });
    }

    private void sendHeartbeat() {
        serverConnections.forEach((remoteAddress, connection) -> {
            votedFor = localAddress;
            AppendEntries appendEntries = new AppendEntries(currentTerm);
            connection.writeBytes(Observable.just(SerializationUtils.serialize(appendEntries)))
                    .take(1)
                    .toBlocking()
                    .forEach(v -> LOGGER.info("Heartbeat sent"));
        });
    }

    private int calculateElectionTimeout() {
        return MIN_ELECTION_TIMEOUT_MILLIS + RAND.nextInt(MAX_ELECTION_TIMEOUT_MILLIS - MIN_ELECTION_TIMEOUT_MILLIS);
    }
}
