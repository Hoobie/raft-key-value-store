package pl.edu.agh;

import org.junit.*;
import pl.edu.agh.utils.ThreadUtils;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RaftServerTest {

    private ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void shouldElectOnlyOneLeader() throws Exception {
        // given
        RaftServer[] nodes = new RaftServer[2];
        executor.execute(() -> nodes[0] = new RaftServer("localhost", 12345, "localhost:12346"));
        executor.execute(() -> nodes[1] = new RaftServer("localhost", 12346, "localhost:12345"));

        // when
        ThreadUtils.sleep(5000L);

        // then
        long leadersAmount = Arrays.stream(nodes)
                .filter(node -> node.getState() == State.LEADER)
                .count();
        Assert.assertEquals(leadersAmount, 1);
    }

    @Test
    public void shouldRecoverFromLeaderCrash() throws Exception {
        // given
        RaftServer[] nodes = new RaftServer[3];
        executor.execute(() -> nodes[0] = new RaftServer("localhost", 12347, "localhost:12348", "localhost:12349"));
        executor.execute(() -> nodes[1] = new RaftServer("localhost", 12348, "localhost:12347", "localhost:12349"));
        executor.execute(() -> nodes[2] = new RaftServer("localhost", 12349, "localhost:12347", "localhost:12348"));

        // when
        // Wait for establishing leader
        ThreadUtils.sleep(5000L);
        Optional<RaftServer> optionalLeader = Arrays.stream(nodes)
                .filter(node -> node.getState() == State.LEADER)
                .findFirst();

        if (!optionalLeader.isPresent())
            throw new IllegalStateException("Leader has not been chosen!");

        RaftServer leader = optionalLeader.get();

        // 'Kill' the leader
        leader.simulateCrash();
        // Wait for new leader
        ThreadUtils.sleep(10000);

        // then
        long leadersAmount = Arrays.stream(nodes)
                .filter(node -> node != leader && node.getState() == State.LEADER)
                .count();
        Assert.assertEquals(leadersAmount, 1);
    }
}