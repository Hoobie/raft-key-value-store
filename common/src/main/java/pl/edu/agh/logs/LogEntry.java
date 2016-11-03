package pl.edu.agh.logs;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class LogEntry {
    private long id;
    private KeyValueStoreAction action;
    private String key;
    private String value;

    public LogEntry(long id, KeyValueStoreAction action, String key, String value) {
        this.id = id;
        this.action = action;
        this.key = key;
        this.value = value;
    }

    /**
     * Constructor for event of removing value (new value is not needed here)
     */
    public LogEntry(long id, KeyValueStoreAction action, String key) {
        this.id = id;
        this.action = action;
        this.key = key;
    }

    public long getId() {
        return id;
    }

    public KeyValueStoreAction getAction() {
        return action;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        if (action == KeyValueStoreAction.REMOVE)
            throw new IllegalStateException("There is no new value when removing from key value store!");
        return value;
    }
}
