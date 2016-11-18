package pl.edu.agh.messages.client;

import pl.edu.agh.messages.RaftMessage;

public class RemoveValueResponse implements RaftMessage {
    private boolean successful;

    public RemoveValueResponse(boolean successful) {
        this.successful = successful;
    }

    public boolean isSuccessful() {
        return successful;
    }
}
