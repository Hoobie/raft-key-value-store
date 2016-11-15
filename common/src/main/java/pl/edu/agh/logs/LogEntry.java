package pl.edu.agh.logs;

import com.google.common.base.MoreObjects;
import pl.edu.agh.messages.RaftMessage;

public class LogEntry implements RaftMessage {
    private long id = -1;
    private int term;
    private KeyValueStoreAction action;
    private String key;
    private int value;

    public LogEntry(KeyValueStoreAction action, String key, int value, int term) {
        this.action = action;
        this.key = key;
        this.value = value;
        this.term = term;
    }

    /**
     * Constructor for event of removing value (new value is not needed here)
     */
    public LogEntry(KeyValueStoreAction action, String key, int term) {
        this.action = action;
        this.key = key;
        this.term = term;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public int getTerm() {
        return term;
    }

    public KeyValueStoreAction getAction() {
        return action;
    }

    public String getKey() {
        return key;
    }

    public int getValue() {
        if (action == KeyValueStoreAction.REMOVE)
            throw new IllegalStateException("There is no new value when removing from key value store!");
        return value;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("Action", action)
                .add("Key", key)
                .add("Value", value)
                .toString();
    }
}
