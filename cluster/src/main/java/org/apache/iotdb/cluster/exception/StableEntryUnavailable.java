package org.apache.iotdb.cluster.exception;

public class StableEntryUnavailable extends Exception {
    public StableEntryUnavailable() {
        super("requested entry at index is unavailable");
    }
}
