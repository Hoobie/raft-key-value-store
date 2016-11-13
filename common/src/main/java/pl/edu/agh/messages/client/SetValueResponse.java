package pl.edu.agh.messages.client;

import pl.edu.agh.messages.RaftMessage;

public class SetValueResponse implements RaftMessage {
    private boolean successful;

    public SetValueResponse(boolean successful) {
        this.successful = successful;
    }

    public boolean isSuccessful() {
        return successful;
    }
}
