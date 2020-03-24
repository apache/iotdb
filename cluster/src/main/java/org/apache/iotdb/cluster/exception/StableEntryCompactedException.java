package org.apache.iotdb.cluster.exception;

public class StableEntryCompactedException extends Exception {
    public StableEntryCompactedException() {
        super("requested index is unavailable due to compaction");
    }
}
