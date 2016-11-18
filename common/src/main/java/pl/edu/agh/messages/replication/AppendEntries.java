package pl.edu.agh.messages.replication;

import pl.edu.agh.logs.LogEntry;
import pl.edu.agh.messages.RaftMessage;

public class AppendEntries implements RaftMessage {
    public int term;
    public LogEntry logEntry;

    // Heartbeat entry
    public AppendEntries(int term) {
        this.term = term;
    }

    public AppendEntries(LogEntry logEntry) {
        this.logEntry = logEntry;
    }

    public LogEntry getLogEntry() {
        return logEntry;
    }
}
