package pl.edu.agh.messages;

import java.io.Serializable;

public class RequestVote implements Serializable {
    public int term;
    public String candidateAddress;
    // TODO: add last log index and term

    public RequestVote(int term, String candidateAddress) {
        this.term = term;
        this.candidateAddress = candidateAddress;
    }
}
