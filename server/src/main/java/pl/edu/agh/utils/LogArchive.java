package pl.edu.agh.utils;

import pl.edu.agh.logs.LogEntry;

import java.util.*;

public class LogArchive {
    private static final int NO_SERVERS_RECEIVED_ENTRY = 0;

    private final Map<LogEntry, Integer> pendingEntries;
    private final List<LogEntry> commitedEntries;

    public LogArchive() {
        pendingEntries = new HashMap<>();
        commitedEntries = new ArrayList<>();
    }

    public LogEntry appendLog(LogEntry entry) {
        entry.setId(getLasLogIdx() + 1);
        pendingEntries.put(entry, NO_SERVERS_RECEIVED_ENTRY);
        return entry;
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

    public int getLasLogIdx() {
        return pendingEntries.size() + commitedEntries.size();
    }

    public int getLastLogTerm() {
        Optional<LogEntry> pendingEntryWithMaxTerm = pendingEntries.keySet().stream().max((l1, l2) -> Integer.valueOf(l1.getTerm()).compareTo(l2.getTerm()));
        Optional<LogEntry> commitedEntryWithMaxTerm = commitedEntries.stream().max((l1, l2) -> Integer.valueOf(l1.getTerm()).compareTo(l2.getTerm()));
        int maxTerm = Integer.MIN_VALUE;
        maxTerm = (pendingEntryWithMaxTerm.isPresent()) ? pendingEntryWithMaxTerm.get().getTerm() : maxTerm;
        maxTerm = (commitedEntryWithMaxTerm.isPresent()) ? Math.max(maxTerm, pendingEntryWithMaxTerm.get().getTerm()) : maxTerm;

        return maxTerm;
    }
}
