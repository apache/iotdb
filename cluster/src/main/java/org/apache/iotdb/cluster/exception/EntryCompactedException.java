package org.apache.iotdb.cluster.exception;

public class EntryCompactedException extends Exception {
    public EntryCompactedException(long index, long boundary) {
        super(String.format("Entry index %d is unavailable due to compaction, and the lower bound is %d", index, boundary));
    }
}