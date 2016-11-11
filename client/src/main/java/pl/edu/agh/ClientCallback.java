package pl.edu.agh;

/**
 * Created by Andrzej on 2016-11-11.
 */
public interface ClientCallback {
    void onValueSet(boolean isSuccess);
    void onValueRemoved(boolean isSuccess);
    void onValueGet(int value);
}
