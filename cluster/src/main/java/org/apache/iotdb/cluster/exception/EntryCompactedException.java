package org.apache.iotdb.cluster.exception;

public class EntryCompactedException extends Exception {
    public EntryCompactedException() {
        super("requested index is unavailable due to compaction");
    }
}
