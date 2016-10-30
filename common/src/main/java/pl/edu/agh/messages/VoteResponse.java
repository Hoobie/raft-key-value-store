package pl.edu.agh.messages;

import java.io.Serializable;

public class VoteResponse implements Serializable {
    public boolean granted;
    // TODO: add term

    public VoteResponse(boolean granted) {
        this.granted = granted;
    }
}
