package pl.edu.agh.messages.client_communication;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class RemoveValue implements ClientMessage {
    private String key;

    public RemoveValue(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
