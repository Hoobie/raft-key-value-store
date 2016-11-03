package pl.edu.agh.messages.replication;

import pl.edu.agh.logs.LogEntry;
import pl.edu.agh.messages.RaftMessage;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class CommitEntry implements RaftMessage {
    private LogEntry logEntry;

    public CommitEntry(LogEntry logEntry) {
        this.logEntry = logEntry;
    }

    public LogEntry getLogEntry() {
        return logEntry;
    }
}
