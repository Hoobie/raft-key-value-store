package pl.edu.agh.messages.client;

public class KeyNotInStoreResponse implements ClientMessage {
    private String key;

    public KeyNotInStoreResponse(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
