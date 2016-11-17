package pl.edu.agh;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.*;
import pl.edu.agh.logs.KeyValueStoreAction;
import pl.edu.agh.utils.SocketAddressUtils;
import pl.edu.agh.utils.ThreadUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RaftClientTest {

    private static final int VALUE = 1;
    private static final String KEY = "test";
    private static final String NOT_EXISTING_KEY = "test2";

    private int responseReceived = 0;
    private RaftServer[] nodes;
    private ExecutorService executor;
    private static int currentPort = 12345;

    @Before
    public void setupServers() {
        nodes = new RaftServer[2];
        executor = Executors.newCachedThreadPool();
        int server1Port = currentPort++;
        int server2Port = currentPort;
        executor.execute(() -> {
            nodes[0] = new RaftServer("localhost", server1Port, String.format("localhost:%s", server2Port));
        });
        executor.execute(() -> {
            nodes[1] = new RaftServer("localhost", server2Port, String.format("localhost:%s", server1Port));
        });

        ThreadUtils.sleep(5000L);
        currentPort++;
    }

    @After
    public void shutDownServers() {
        Arrays.stream(nodes).forEach(RaftServer::simulateCrash);
    }

    @Test
    public void shouldGetCorrectValuesFromServer() {
        // given
        responseReceived = 0;

        // when
        new RaftClient(KeyValueStoreAction.SET, KEY, VALUE, correctResponseCallback, getServerAddresses());
        ThreadUtils.sleep(5000);
        new RaftClient(KeyValueStoreAction.GET, KEY, null, correctResponseCallback, getServerAddresses());
        ThreadUtils.sleep(5000);
        new RaftClient(KeyValueStoreAction.REMOVE, KEY, null, correctResponseCallback, getServerAddresses());

        // then
        // Wait for responses
        ThreadUtils.sleep(5000);
        assertEquals(responseReceived, 3);
    }

    @Test
    public void shouldNotCrashIfKeyNotInStore() {
        // given
        responseReceived = 0;

        // when
        new RaftClient(KeyValueStoreAction.GET, NOT_EXISTING_KEY, null, keyNotInStoreCallback, getServerAddresses());

        // then
        // Wait for response
        ThreadUtils.sleep(5000);
        assertEquals(responseReceived, 1);
    }

    @Test
    public void shouldGetCorrectValueAfterLeaderCrash() {
        // given
        // Value set while in contact with current leader
        responseReceived = 0;
        new RaftClient(KeyValueStoreAction.SET, KEY, VALUE, correctResponseCallback, getServerAddresses());
        // Wait for response
        ThreadUtils.sleep(5000);
        assertEquals(responseReceived, 1);

        // when
        // Leader is killed
        Optional<RaftServer> optionalLeader = Arrays.stream(nodes)
                .filter(node -> node.getState() == State.LEADER)
                .findFirst();
        if (!optionalLeader.isPresent())
            throw new IllegalStateException("Leader has not been chosen!");

        RaftServer leader = optionalLeader.get();
        leader.simulateCrash();

        // Wait for new leader and try to get correct response
        ThreadUtils.sleep(5000);
        responseReceived = 0;
        new RaftClient(KeyValueStoreAction.GET, KEY, null, correctResponseCallback, getServerAddresses());

        // then
        // Wait for response
        ThreadUtils.sleep(5000);
        assertEquals(responseReceived, 1);

    }

    @Test
    public void shouldCommitChangeOnAllServers() {
        // given
        // Value set while in contact with current leader
        responseReceived = 0;
        new RaftClient(KeyValueStoreAction.SET, KEY, VALUE, correctResponseCallback, getServerAddresses());
        // Wait for response
        ThreadUtils.sleep(5000);
        assertEquals(responseReceived, 1);

        // then
        Arrays.stream(nodes).forEach(node -> Assert.assertTrue(node.getStateMachine().containsKey(KEY) && node.getStateMachine().get(KEY) == VALUE));

    }

    @Test
    public void shouldGetUpdatedWithLogsAfterServerRevive() {
        // given
        // Value set while in contact with current leader
        responseReceived = 0;
        new RaftClient(KeyValueStoreAction.SET, KEY, VALUE, correctResponseCallback, getServerAddresses());
        // Wait for response
        ThreadUtils.sleep(5000L);
        assertEquals(responseReceived, 1);

        // when
        // Kill server make change in statemachine of current leader and than revive it
        // TODO: Kill the server for good

        responseReceived = 0;
        new RaftClient(KeyValueStoreAction.REMOVE, KEY, null, correctResponseCallback, getServerAddresses());
        // Wait for response
        ThreadUtils.sleep(5000L);
        assertEquals(responseReceived, 1);

        // TODO: Revive it somehow w/o 'BindException: Address already in use'
        ThreadUtils.sleep(5000L);
        Assert.assertTrue(nodes[0].getStateMachine().containsKey(KEY) && nodes[0].getStateMachine().get(KEY) == VALUE);
    }

    private String[] getServerAddresses() {
        String[] serverAddresses = new String[2];
        serverAddresses[0] = String.format("localhost:%s", currentPort - 1);
        serverAddresses[1] = String.format("localhost:%s", currentPort - 2);
        return serverAddresses;
    }

    private ClientCallback correctResponseCallback = new ClientCallback() {
        @Override
        public void onValueSet(boolean isSuccess) {
            assertTrue(isSuccess);
            responseReceived++;
        }

        @Override
        public void onValueRemoved(boolean isSuccess) {
            assertTrue(isSuccess);
            responseReceived++;
        }

        @Override
        public void onValueGet(int value) {
            assertEquals(value, VALUE);
            responseReceived++;
        }

        @Override
        public void onKeyNotInStore(String key) {
            assertTrue(String.format("Key %s should be in store!", key), false);
        }
    };

    private ClientCallback keyNotInStoreCallback = new ClientCallback() {
        @Override
        public void onValueSet(boolean isSuccess) {
            assertTrue("Wrong message!", false);
        }

        @Override
        public void onValueRemoved(boolean isSuccess) {
            assertTrue("Wrong message!", false);
        }

        @Override
        public void onValueGet(int value) {
            assertTrue("Wrong message!", false);
        }

        @Override
        public void onKeyNotInStore(String key) {
            assertEquals(key, NOT_EXISTING_KEY);
            responseReceived++;
        }
    };
}
