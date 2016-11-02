package pl.edu.agh.messages.replication;

import pl.edu.agh.messages.RaftMessage;

public class AppendEntries implements RaftMessage {
    public int term;

    public AppendEntries(int term) {
        this.term = term;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }
}
