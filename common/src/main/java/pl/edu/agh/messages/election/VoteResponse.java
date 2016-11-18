package pl.edu.agh.messages.election;

import pl.edu.agh.messages.RaftMessage;

public class VoteResponse implements RaftMessage {
    public int term;
    public boolean granted;

    public VoteResponse(int term, boolean granted) {
        this.term = term;
        this.granted = granted;
    }
}
