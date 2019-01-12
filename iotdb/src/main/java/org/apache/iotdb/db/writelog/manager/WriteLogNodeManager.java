package org.apache.iotdb.db.writelog.manager;

import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.db.exception.RecoverException;

import java.io.IOException;

/**
 * This interface provides accesses to WriteLogNode.
 */
public interface WriteLogNodeManager {

    /**
     * Get a WriteLogNode by a identifier like "{storageGroupName}-bufferwrite/overflow".
     * The WriteLogNode will be automatically created if not exist and restoreFilePath and processorStoreFilePath are provided,
     * if either restoreFilePath or processorStoreFilePath is not provided and the LogNode does not exist, null is returned.
     * @param identifier
     * @param processorStoreFilePath
     * @param restoreFilePath
     * @return
     */
    WriteLogNode getNode(String identifier, String restoreFilePath, String processorStoreFilePath) throws IOException;

    /**
     * Delete a log node. If the node log does not exist, this will be an empty operation.
     * @param identifier
     */
    void deleteNode(String identifier) throws IOException;

    /**
     * Make all node of this manager start recovery.
     */
    void recover() throws RecoverException;

    /**
     * Close all nodes.
     */
    void close();

    /**
     *
     * @param fileNodeName
     * @return Whether WAL files exist for certain fileNode/
     */
    boolean hasWAL(String fileNodeName);
}
