package pl.edu.agh;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import io.reactivex.netty.protocol.tcp.server.TcpServer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.agh.logs.KeyValueStoreAction;
import pl.edu.agh.logs.LogArchive;
import pl.edu.agh.logs.LogEntry;
import pl.edu.agh.messages.RaftMessage;
import pl.edu.agh.messages.RequestLogs;
import pl.edu.agh.messages.client.*;
import pl.edu.agh.messages.election.RequestVote;
import pl.edu.agh.messages.election.VoteResponse;
import pl.edu.agh.messages.replication.AppendEntries;
import pl.edu.agh.messages.replication.AppendEntriesResponse;
import pl.edu.agh.messages.replication.CommitEntry;
import pl.edu.agh.utils.MessageUtils;
import pl.edu.agh.utils.SocketAddressUtils;
import rx.Observable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static javaslang.API.*;
import static javaslang.Predicates.instanceOf;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static pl.edu.agh.utils.ThreadUtils.sleep;

public class RaftServer {

    private static final int MIN_ELECTION_TIMEOUT_MILLIS = 150;
    private static final int MAX_ELECTION_TIMEOUT_MILLIS = 300;
    private static final int HEARTBEAT_TIMEOUT_MILLIS = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftServer.class);

    private static final Random RAND = new Random();
    private static final ScheduledExecutorService TIMEOUT_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    private final SocketAddress localAddress;
    private final Map<SocketAddress, Connection<ByteBuf, ByteBuf>> serverConnections;
    private Connection<ByteBuf, ByteBuf> clientConnection;

    private State state = State.FOLLOWER;
    private int currentTerm = 0;
    private AtomicInteger votesCount = new AtomicInteger(0);
    private SocketAddress votedFor;
    private ScheduledFuture timeout;

    private final Map<String, Integer> keyValueStore = new HashMap<>();
    private final LogArchive logArchive = new LogArchive();
    private final Queue<RaftMessage> messagesToNeighbors = new ConcurrentLinkedQueue<>();

    public static void main(final String[] args) {
        Pair<String, Integer> localAddress = SocketAddressUtils.splitHostAndPort(args[0]);
        new RaftServer(localAddress.getLeft(), localAddress.getRight(),
                ArrayUtils.subarray(args, 1, args.length));
    }

    public RaftServer(String host, int port, String... serversHostsAndPorts) {
        createTcpServer(port);
        this.localAddress = new InetSocketAddress(host, port);

        serverConnections = Arrays.stream(serversHostsAndPorts)
                .map(SocketAddressUtils::splitHostAndPort)
                .map(hostAndPort -> {
                    Connection<ByteBuf, ByteBuf> tcpConnection = createTcpConnection(hostAndPort.getLeft(), hostAndPort.getRight());
                    return Pair.of(tcpConnection.getChannelPipeline().channel().remoteAddress(), tcpConnection);
                })
                .collect(Collectors.toConcurrentMap(Pair::getLeft, Pair::getRight));

        messagesToNeighbors.add(new RequestLogs());
        timeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateElectionTimeout(), TimeUnit.MILLISECONDS);
    }

    public State getState() {
        return state;
    }

    private TcpServer<ByteBuf, ByteBuf> createTcpServer(int port) {
        TcpServer<ByteBuf, ByteBuf> tcpServer = TcpServer.newServer(port);
        tcpServer.enableWireLogging("server", LogLevel.DEBUG)
                .start(connection -> {
                            if (!checkIfClient(connection)) {
                                SocketAddress remoteAddress = connection.getChannelPipeline().channel().remoteAddress();
                                if (serverConnections != null) {
                                    Optional<SocketAddress> c = serverConnections.keySet().stream()
                                            .filter(k -> k.equals(remoteAddress))
                                            .findFirst();
                                    if (c.isPresent()) {
                                        serverConnections.remove(c.get());
                                        serverConnections.put(remoteAddress, connection);
                                        LOGGER.info("Replaced a server's connection {}", remoteAddress);
                                    }
                                }
                            }
                            return connection.writeStringAndFlushOnEach(connection.getInput()
                                    .map(bb -> {
                                        String stringRepr = bb.toString(Charset.defaultCharset());
                                        bb.release();
                                        return stringRepr;
                                    })
                                    .map(MessageUtils::toObject)
                                    .map(o -> handleRequest(o, connection))
                                    .filter(Optional::isPresent)
                                    .map(Optional::get)
                                    .map(MessageUtils::toString)
                                    .onErrorReturn(e -> {
                                        LOGGER.error("Server handling error", e);
                                        return EMPTY;
                                    })
                            );
                        }
                );
        return tcpServer;
    }

    private void bringUpToDate(Connection<ByteBuf, ByteBuf> connection) {
        SocketAddress remoteAddress = connection.getChannelPipeline().channel().remoteAddress();
        LOGGER.info("Bringing server {} up to date", remoteAddress);
        logArchive.getCommittedLogs().forEach(log ->
                connection.writeStringAndFlushOnEach(Observable.just(MessageUtils.toString(new CommitEntry(log))))
                        .subscribe(
                                n -> LOGGER.info("CommitEntry {} sent to {} as an update", log, remoteAddress),
                                e -> LOGGER.error("Error on sending CommitEntry to {}", remoteAddress)
                        ));
    }

    private boolean checkIfClient(Connection<ByteBuf, ByteBuf> connection) {
        long count = (serverConnections == null) ? 0 : serverConnections.keySet()
                .stream()
                .filter(socketAddress ->
                        !socketAddress.toString().equals(connection.getChannelPipeline().channel().remoteAddress().toString()))
                .count();

        if (count > 0) {
            clientConnection = connection;
            return true;
        }
        return false;
    }

    private Optional<RaftMessage> handleRequest(Object obj, Connection<ByteBuf, ByteBuf> connection) {
        return Match(obj).of(
                Case(instanceOf(RequestVote.class), rv -> {
                    boolean granted = false;
                    if (currentTerm <= rv.term && (votedFor == null || votedFor == rv.candidateAddress)
                            && isAtLeastUpToDateAsCandidate(rv)) {
                        votedFor = rv.candidateAddress;
                        granted = true;
                    }
                    VoteResponse response = new VoteResponse(currentTerm, granted);
                    return Optional.of(response);
                }),
                Case(instanceOf(AppendEntries.class), ae -> {
                    LogEntry logEntry = ae.getLogEntry();
                    if (logEntry == null) {
                        // Heartbeat
                        AppendEntriesResponse response = new AppendEntriesResponse();
                        if (ae.term >= currentTerm) {
                            if (state != State.FOLLOWER) LOGGER.info("The leader have spoken, becoming a follower...");
                            else LOGGER.debug("The leader have spoken");

                            state = State.FOLLOWER;
                            votedFor = null;
                            if (timeout != null)
                                timeout.cancel(false);
                            timeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateElectionTimeout(), TimeUnit.MILLISECONDS);
                        }
                        return Optional.of(response);
                    } else {
                        if (timeout != null)
                            timeout.cancel(false);
                        timeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateElectionTimeout(), TimeUnit.MILLISECONDS);
                        return handleLogEntry(logEntry);
                    }
                }),
                Case(instanceOf(CommitEntry.class), ce -> {
                    if (timeout != null)
                        timeout.cancel(false);
                    timeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateElectionTimeout(), TimeUnit.MILLISECONDS);
                    LogEntry entry = ce.getLogEntry();
                    commitEntry(entry);
                    return Optional.empty();
                }),
                Case(instanceOf(ClientMessage.class), cm -> {
                    if (state == State.LEADER)
                        return handleClientMessage(cm);
                    return Optional.empty();
                }),
                Case(instanceOf(RequestLogs.class), rl -> {
                    if (state == State.LEADER)
                        bringUpToDate(connection);
                    return Optional.empty();

                }),
                Case($(), o -> Optional.empty())
        );
    }

    private boolean isAtLeastUpToDateAsCandidate(RequestVote candidatesRequest) {
        // Based on 5.4.2 paragraph of the Raft paper
        return candidatesRequest.lastLogTerm > logArchive.getLastLogTerm() ||
                (candidatesRequest.lastLogTerm == logArchive.getLastLogTerm() && candidatesRequest.lastLogIndex >= logArchive.getLastLogIdx());
    }

    private void commitEntry(LogEntry entry) {
        LOGGER.info("Commit entry: " + entry);
        if (logArchive.containsCommittedLogEntry(entry)) return;
        logArchive.commitEntry(entry);
        switch (entry.getAction()) {
            case SET:
                keyValueStore.put(entry.getKey(), entry.getValue());
                break;
            case REMOVE:
                keyValueStore.remove(entry.getKey());
                break;
        }
    }

    private Optional<RaftMessage> handleLogEntry(LogEntry entry) {
        LOGGER.info("Received logEntry: " + entry);
        entry = logArchive.appendLog(entry);
        AppendEntriesResponse response = new AppendEntriesResponse(entry);
        return Optional.of(response);
    }

    private Optional<RaftMessage> handleClientMessage(ClientMessage cm) {
        LOGGER.info("I'm a leader ({}) and I got this client message: {}", localAddress, cm.toString());

        return Match(cm).of(
                Case(instanceOf(GetValue.class), gv -> {
                    if (!keyValueStore.containsKey(gv.getKey()))
                        return Optional.of(new KeyNotInStoreResponse(gv.getKey()));
                    GetValueResponse response = new GetValueResponse(keyValueStore.get(gv.getKey()));
                    return Optional.of(response);
                }),
                Case(instanceOf(SetValue.class), sv -> {
                    LogEntry entry = new LogEntry(KeyValueStoreAction.SET, sv.getKey(), sv.getValue(), currentTerm);
                    entry = logArchive.appendLog(entry);
                    messagesToNeighbors.add(new AppendEntries(entry));
                    return Optional.empty();
                }),
                Case(instanceOf(RemoveValue.class), rv -> {
                    if (!keyValueStore.containsKey(rv.getKey()))
                        return Optional.of(new KeyNotInStoreResponse(rv.getKey()));
                    LogEntry entry = new LogEntry(KeyValueStoreAction.REMOVE, rv.getKey(), currentTerm);
                    entry = logArchive.appendLog(entry);
                    messagesToNeighbors.add(new AppendEntries(entry));
                    return Optional.empty();
                }),
                Case($(), o -> Optional.empty())
        );
    }

    private Connection<ByteBuf, ByteBuf> createTcpConnection(String address, int port) {
        try {
            Connection<ByteBuf, ByteBuf> connection = TcpClient.newClient(address, port)
                    .enableWireLogging("server-connection", LogLevel.DEBUG)
                    .createConnectionRequest()
                    .toBlocking()
                    .first();

            connection.getInput()
                    .subscribe(
                            byteBuf -> {
                                handleResponse(MessageUtils.toObject(byteBuf.toString(Charset.defaultCharset())));
                                byteBuf.release();
                            },
                            error -> LOGGER.error("Error on handling response from {}",
                                    connection.getChannelPipeline().channel().remoteAddress(), error)
                    );

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
                        votesCount = new AtomicInteger(0);
                        timeout.cancel(false);
                        timeout = TIMEOUT_EXECUTOR.scheduleAtFixedRate(this::handleTimeout, 0,
                                HEARTBEAT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                    }
                    return Optional.empty();
                }),
                Case(instanceOf(AppendEntriesResponse.class), aer -> {
                    LogEntry entry = aer.getEntry();
                    if (entry == null) {
                        // Heartbeat response
                    } else {
                        handleLogEntryResponse(entry);
                    }
                    LOGGER.debug("AppendEntriesResponse received");
                    return Optional.empty();
                }),
                Case($(), o -> Optional.empty())
        );
    }

    private void handleLogEntryResponse(LogEntry entry) {
        LOGGER.info("Received logEntryResponse: " + entry);

        int responsesCount = logArchive.logEntryReceived(entry);
        if (isMajority(responsesCount)) {
            commitEntry(entry);
            RaftMessage response = null;
            if (entry.getAction() == KeyValueStoreAction.REMOVE)
                response = new RemoveValueResponse(true);
            else if (entry.getAction() == KeyValueStoreAction.SET)
                response = new SetValueResponse(true);

            if (response != null && clientConnection != null) {
                clientConnection.writeStringAndFlushOnEach(Observable.just(MessageUtils.toString(response)))
                        .subscribe(
                                n -> LOGGER.info("Response sent to client {}",
                                        clientConnection.getChannelPipeline().channel().remoteAddress()),
                                e -> LOGGER.error("Error on sending response to client {}",
                                        clientConnection.getChannelPipeline().channel().remoteAddress(), e)
                        );
            }

            messagesToNeighbors.add(new CommitEntry(entry));
        }
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
                sendMessageToNeighbors();
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void startElection() {
        currentTerm++;
        timeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateElectionTimeout(), TimeUnit.MILLISECONDS);
        serverConnections.forEach((remoteAddress, connection) -> {
            votedFor = null;
            RequestVote requestVote = new RequestVote(currentTerm, localAddress, logArchive.getLastLogIdx(), logArchive.getLastLogTerm());
            connection.writeStringAndFlushOnEach(Observable.just(MessageUtils.toString(requestVote)))
                    .subscribe(
                            n -> LOGGER.info("RequestVote sent to {}", remoteAddress),
                            e -> LOGGER.error("Error on sending RequestVote to {}", remoteAddress)
                    );
        });
    }

    private void sendMessageToNeighbors() {
        RaftMessage message = (messagesToNeighbors.size() > 0) ? messagesToNeighbors.remove() : new AppendEntries(currentTerm);

        serverConnections.forEach((remoteAddress, connection) ->
                connection.writeStringAndFlushOnEach(Observable.just(MessageUtils.toString(message)))
                        .subscribe(
                                n -> LOGGER.info("Message {} sent to {}", message, remoteAddress),
                                e -> LOGGER.error("Error on sending AppendEntries to {}", remoteAddress)
                        )
        );
    }

    private int calculateElectionTimeout() {
        return MIN_ELECTION_TIMEOUT_MILLIS + RAND.nextInt(MAX_ELECTION_TIMEOUT_MILLIS - MIN_ELECTION_TIMEOUT_MILLIS);
    }

    @VisibleForTesting
    void simulateCrash() {
        timeout.cancel(true);
    }

    public Map<String, Integer> getStateMachine() {
        return keyValueStore;
    }

    public void restart() {
        messagesToNeighbors.add(new RequestLogs());
        timeout = TIMEOUT_EXECUTOR.schedule(this::handleTimeout, calculateElectionTimeout(), TimeUnit.MILLISECONDS);
    }
}
