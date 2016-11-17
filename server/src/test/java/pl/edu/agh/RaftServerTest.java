package pl.edu.agh;

import org.junit.Assert;
import org.junit.Test;
import pl.edu.agh.utils.ThreadUtils;

import java.util.Arrays;
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
        ThreadUtils.sleep(5000L);

        // then
        long leadersAmount = Arrays.stream(nodes)
                .filter(node -> node.getState() == State.LEADER)
                .count();
        Assert.assertEquals(leadersAmount, 1);
    }
}