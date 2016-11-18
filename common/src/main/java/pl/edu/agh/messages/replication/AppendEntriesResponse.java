package pl.edu.agh.messages.replication;

import pl.edu.agh.logs.LogEntry;
import pl.edu.agh.messages.RaftMessage;

public class AppendEntriesResponse implements RaftMessage {
    private LogEntry entry;

    // Solely for heartbeat responses
    public AppendEntriesResponse() {}

    public AppendEntriesResponse(LogEntry entry) {
        this.entry = entry;
    }

    public LogEntry getEntry() {
        return entry;
    }
}
