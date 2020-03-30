package org.apache.iotdb.cluster.exception;

public class EntryUnavailableException extends Exception {
    public EntryUnavailableException(long index, long boundary) {
        super(String.format("Entry index %d is unavailable, and the upper bound is %d", index, boundary));
    }
}