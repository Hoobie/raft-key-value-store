package pl.edu.agh.logs;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class LogEntry {
    private long id = -1;
    private KeyValueStoreAction action;
    private String key;
    private String value;

    public LogEntry(KeyValueStoreAction action, String key, String value) {
        this.action = action;
        this.key = key;
        this.value = value;
    }

    /**
     * Constructor for event of removing value (new value is not needed here)
     */
    public LogEntry(KeyValueStoreAction action, String key) {
        this.action = action;
        this.key = key;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        if (id == -1) throw new IllegalStateException("Log entry ID has to be set!");
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
