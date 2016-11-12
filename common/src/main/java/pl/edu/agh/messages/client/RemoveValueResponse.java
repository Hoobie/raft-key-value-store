package pl.edu.agh.messages.client;

import pl.edu.agh.messages.RaftMessage;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class RemoveValueResponse implements RaftMessage {
    private boolean successful;

    public RemoveValueResponse(boolean successful) {
        this.successful = successful;
    }

    public boolean isSuccessful() {
        return successful;
    }
}
