package pl.edu.agh;

import org.junit.Assert;
import org.junit.Test;
import pl.edu.agh.utils.ThreadUtils;

public class RaftServerTest {

    @Test
    public void shouldElectOnlyOneLeader() throws Exception {
        // given
        RaftServer[] nodes = new RaftServer[2];
        new Thread() {
            @Override
            public void run() {
                nodes[0] = new RaftServer("localhost", 12345, "localhost:12346");
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                nodes[1] = new RaftServer("localhost", 12346, "localhost:12345");
            }
        }.start();

        // when
        ThreadUtils.sleep(2000L);

        // then
        Assert.assertTrue(nodes[0].getState() == State.LEADER || nodes[1].getState() == State.LEADER);
        Assert.assertFalse(nodes[0].getState() == State.LEADER && nodes[1].getState() == State.LEADER);
    }
}