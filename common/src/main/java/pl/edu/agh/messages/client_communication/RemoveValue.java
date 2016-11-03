package pl.edu.agh.messages.client_communication;

import pl.edu.agh.messages.RaftMessage;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class RemoveValue implements RaftMessage {
    private String key;

    public RemoveValue(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
