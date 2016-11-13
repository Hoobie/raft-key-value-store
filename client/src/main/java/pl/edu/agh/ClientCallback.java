package pl.edu.agh;

public interface ClientCallback {
    void onValueSet(boolean isSuccess);
    void onValueRemoved(boolean isSuccess);
    void onValueGet(int value);
    void onKeyNotInStore(String key);
}
