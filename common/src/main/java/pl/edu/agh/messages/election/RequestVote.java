package pl.edu.agh.messages.election;

import pl.edu.agh.messages.RaftMessage;

import java.net.SocketAddress;

public class RequestVote implements RaftMessage {
    public int term;
    public SocketAddress candidateAddress;
    public int lastLogIndex;
    public int lastLogTerm;

    public RequestVote(int term, SocketAddress candidateAddress, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateAddress = candidateAddress;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
}
