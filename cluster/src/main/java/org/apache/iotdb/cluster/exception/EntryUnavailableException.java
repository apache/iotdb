package org.apache.iotdb.cluster.exception;

public class EntryUnavailableException extends Exception {
    public EntryUnavailableException() {
        super("requested entry at index is unavailable");
    }
}