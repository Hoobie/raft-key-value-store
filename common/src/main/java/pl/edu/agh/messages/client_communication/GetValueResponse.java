package pl.edu.agh.messages.client_communication;

import pl.edu.agh.messages.RaftMessage;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class GetValueResponse implements RaftMessage {
    private int value;

    public GetValueResponse(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
