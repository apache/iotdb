package org.apache.iotdb.db.writelog.node;

import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.writelog.LogPosition;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.exception.RecoverException;

import java.io.IOException;
import java.util.List;

public interface WriteLogNode {

    /**
     * Write a log which implements LogSerializable.
     * First, the log will be conveyed to byte[] by codec. Then the byte[] will be put into a cache.
     * If necessary, the logs in the cache will be synced to disk.
     * @param plan
     * @return The position to be written of the log.
     */
    LogPosition write(PhysicalPlan plan) throws IOException;

    /**
     * First judge the stage of recovery by status of files, and then recover from that stage.
     */
    void recover() throws RecoverException;

    /**
     * Sync and close streams.
     */
    void close() throws IOException;

    /**
     * Write what in cache to disk.
     */
    void forceSync() throws IOException;

    /**
     * When a FileNode attempts to start a flush, this method must be called to rename log file.
     */
    void notifyStartFlush() throws IOException;

    /**
     * When the flush of a FlieNode ends, this method must be called to check if log file needs cleaning.
     */
    void notifyEndFlush(List<LogPosition> logPositions);

    /**
     *
     * @return The identifier of this log node.
     */
    String getIdentifier();

    /**
     *
     * @return The directory where wal file is placed.
     */
    String getLogDirectory();

    /**
     * Abandon all logs in this node and delete the log directory.
     * The caller should guarantee that NO MORE WRITE is coming.
     */
    void delete() throws IOException;
}
