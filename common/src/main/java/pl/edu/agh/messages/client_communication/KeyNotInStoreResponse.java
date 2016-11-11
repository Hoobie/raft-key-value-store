package pl.edu.agh.messages.client_communication;

/**
 * Created by Andrzej on 2016-11-11.
 */
public class KeyNotInStoreResponse implements ClientMessage {
    private String key;

    public KeyNotInStoreResponse(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
