package pl.edu.agh.messages.client;

import pl.edu.agh.messages.RaftMessage;

public class GetValueResponse implements RaftMessage {
    private int value;

    public GetValueResponse(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
