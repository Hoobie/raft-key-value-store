package pl.edu.agh.messages;

public class VoteResponse implements RaftMessage {
    public boolean granted;
    // TODO: add term

    public VoteResponse(boolean granted) {
        this.granted = granted;
    }
}
