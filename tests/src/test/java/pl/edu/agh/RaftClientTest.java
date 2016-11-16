package pl.edu.agh;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.edu.agh.logs.KeyValueStoreAction;
import pl.edu.agh.utils.SocketAddressUtils;
import pl.edu.agh.utils.ThreadUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RaftClientTest {

    private static final int VALUE = 1;
    private static final String KEY = "test";
    private static final String NOT_EXISTING_KEY = "test2";
    private static final String[] SERVER_ADDRESSES = new String[]{"localhost:12345", "localhost:12346"};

    private static int responseReceived = 0;

    @BeforeClass
    public static void setUp() {
        // given
        new Thread() {
            @Override
            public void run() {
                Pair<String, Integer> address = SocketAddressUtils.splitHostAndPort(SERVER_ADDRESSES[0]);
                new RaftServer(address.getLeft(), address.getRight(), SERVER_ADDRESSES[1]);
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                Pair<String, Integer> address = SocketAddressUtils.splitHostAndPort(SERVER_ADDRESSES[1]);
                new RaftServer(address.getLeft(), address.getRight(), SERVER_ADDRESSES[0]);
            }
        }.start();

        // Wait for electing leader
        ThreadUtils.sleep(5000L);
    }

    @Test
    public void shouldGetCorrectValuesFromServer() {
        responseReceived = 0;

        // when
        new RaftClient(KeyValueStoreAction.SET, KEY, VALUE, correctResponseCallback, SERVER_ADDRESSES);
        ThreadUtils.sleep(5000);
        new RaftClient(KeyValueStoreAction.GET, KEY, null, correctResponseCallback, SERVER_ADDRESSES);
        ThreadUtils.sleep(5000);
        new RaftClient(KeyValueStoreAction.REMOVE, KEY, null, correctResponseCallback, SERVER_ADDRESSES);

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
        new RaftClient(KeyValueStoreAction.GET, NOT_EXISTING_KEY, null, keyNotInStoreCallback, SERVER_ADDRESSES);

        // then
        // Wait for response
        ThreadUtils.sleep(5000);
        assertEquals(responseReceived, 1);
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
