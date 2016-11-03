package pl.edu.agh.messages.client_communication;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class SetValue implements ClientMessage {
    private String key;
    private int value;

    public SetValue(String key, int value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }
}
