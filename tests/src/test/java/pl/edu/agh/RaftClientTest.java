package pl.edu.agh;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import pl.edu.agh.utils.SocketAddressUtils;
import pl.edu.agh.utils.ThreadUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RaftClientTest {

    private static final int VALUE = 1;
    private static final String KEY = "test";
    private static final String NOT_EXISTING_KEY = "test2";
    private static final String[] SERVER_ADDRESSES = new String[]{"localhost:12345", "localhost:12346"};

    private static RaftClient client = null;
    private static int responseReceived = 0;
    private static boolean setUp = false;

    @Before
    public void setUp() {
        // given
        if (!setUp) {
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

            new Thread() {
                @Override
                public void run() {
                    client = new RaftClient(SERVER_ADDRESSES);
                }
            }.start();
            ThreadUtils.sleep(1000);
            setUp = true;
        }
    }

    @Test
    public void shouldGetCorrectValuesFromServer() {
        // given
        client.setCallback(correctResponseCallback);

        // when
        client.setValue(KEY, VALUE);
        ThreadUtils.sleep(5000);
        client.getValue(KEY);
        ThreadUtils.sleep(5000);
        client.removeValue(KEY);

        // then
        // Wait for responses
        ThreadUtils.sleep(5000);
        assertEquals(responseReceived, 3);
    }

    @Test
    public void shouldNotCrashIfKeyNotInStore() {
        // given
        responseReceived = 0;
        client.setCallback(keyNotInStoreCallback);

        // when
        client.getValue(NOT_EXISTING_KEY);

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
