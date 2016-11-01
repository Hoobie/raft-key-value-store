package pl.edu.agh.messages.election;

import pl.edu.agh.messages.RaftMessage;

import java.net.SocketAddress;

public class RequestVote implements RaftMessage {
    public int term;
    public SocketAddress candidateAddress;
    // TODO: add last log index and term

    public RequestVote(int term, SocketAddress candidateAddress) {
        this.term = term;
        this.candidateAddress = candidateAddress;
    }
}