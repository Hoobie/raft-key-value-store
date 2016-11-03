package pl.edu.agh.messages.client_communication;

import pl.edu.agh.messages.RaftMessage;

/**
 * Created by Andrzej on 2016-11-03.
 */
public class SetValueResponse implements RaftMessage {
    private boolean successful;

    public SetValueResponse(boolean successful) {
        this.successful = successful;
    }

    public boolean isSuccessful() {
        return successful;
    }
}
