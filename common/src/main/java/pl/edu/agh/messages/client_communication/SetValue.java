package pl.edu.agh.messages.client_communication;

import com.google.common.base.MoreObjects;

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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("KEY", key)
                .add("VALUE", value)
                .toString();
    }
}
