package pl.edu.agh.messages.client;

import com.google.common.base.MoreObjects;

public class RemoveValue implements ClientMessage {
    private String key;

    public RemoveValue(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("KEY", key)
                .toString();
    }
}
