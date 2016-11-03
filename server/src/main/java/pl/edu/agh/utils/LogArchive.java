package pl.edu.agh.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import pl.edu.agh.logs.LogEntry;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class LogArchive {
    private static final int NO_SERVERS_RECEIVED_ENTRY = 0;

    private Map<LogEntry, Integer> pendingEntries;
    private List<LogEntry> commitedEntries;

    public LogArchive() {
        pendingEntries = Maps.newHashMap();
        commitedEntries = Lists.newArrayList();
    }

    public void appendLog(LogEntry entry) {
        entry.setId(pendingEntries.size() + 1);
        pendingEntries.put(entry, NO_SERVERS_RECEIVED_ENTRY);
    }

    public int logEntryReceived(LogEntry entry) {
        Optional<Map.Entry<LogEntry, Integer>> localEntry = pendingEntries
                .entrySet().stream()
                .filter(e -> e.getKey().getId() == entry.getId())
                .findFirst();

        if (localEntry.isPresent()) {
            // Update count of servers that received this entry
            pendingEntries.put(localEntry.get().getKey(), localEntry.get().getValue() + 1);
            return localEntry.get().getValue() + 1;
        }

        // We don't even have this entry
        return NO_SERVERS_RECEIVED_ENTRY;
    }

    public void commitEntry(LogEntry entry) {
        pendingEntries.entrySet().removeIf(e -> e.getKey().getId() == entry.getId());
        commitedEntries.add(entry);
    }
}