package pl.edu.agh.messages.election;

import pl.edu.agh.messages.RaftMessage;

public class VoteResponse implements RaftMessage {
    public boolean granted;
    // TODO: add term

    public VoteResponse(boolean granted) {
        this.granted = granted;
    }
}
