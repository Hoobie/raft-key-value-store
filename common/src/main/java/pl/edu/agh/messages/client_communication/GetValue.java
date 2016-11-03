package pl.edu.agh.messages.client_communication;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class GetValue implements ClientMessage {
    private String key;

    public GetValue(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
