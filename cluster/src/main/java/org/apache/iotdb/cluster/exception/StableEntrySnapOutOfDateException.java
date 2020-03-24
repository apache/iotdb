package org.apache.iotdb.cluster.exception;

public class StableEntrySnapOutOfDateException extends Exception {
    public StableEntrySnapOutOfDateException() {
        super("requested index is older than the existing snapshot");
    }
}
