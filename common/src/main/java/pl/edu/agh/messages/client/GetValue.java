package pl.edu.agh.messages.client;

import com.google.common.base.MoreObjects;

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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("KEY", key)
                .toString();
    }
}
