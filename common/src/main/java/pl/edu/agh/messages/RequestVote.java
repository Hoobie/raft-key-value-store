package pl.edu.agh.messages;

import java.io.Serializable;
import java.net.SocketAddress;

public class RequestVote implements Serializable {
    public int term;
    public SocketAddress candidateAddress;
    // TODO: add last log index and term

    public RequestVote(int term, SocketAddress candidateAddress) {
        this.term = term;
        this.candidateAddress = candidateAddress;
    }
}
