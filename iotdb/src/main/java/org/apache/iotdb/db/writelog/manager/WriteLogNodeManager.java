/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.writelog.manager;

import java.io.IOException;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.writelog.node.WriteLogNode;

/**
 * This interface provides accesses to WriteLogNode.
 */
public interface WriteLogNodeManager {

  /**
   * Get a WriteLogNode by a identifier like "{storageGroupName}-bufferwrite/overflow". The
   * WriteLogNode will be automatically created if not exist and restoreFilePath and
   * processorStoreFilePath are provided, if either restoreFilePath or processorStoreFilePath is not
   * provided and the LogNode does not exist, null is returned.
   *
   * @param identifier -identifier
   * @param processorStoreFilePath -processor store file path
   * @param restoreFilePath -restore file path
   */
  WriteLogNode getNode(String identifier, String restoreFilePath, String processorStoreFilePath)
      throws IOException;

  /**
   * Delete a log node. If the node log does not exist, this will be an empty operation.
   *
   * @param identifier -identifier
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
   * return whether wal files exist for certain fileNode.
   *
   * @param fileNodeName -fileNode Name
   * @return Whether WAL files exist for certain fileNode/
   */
  boolean hasWAL(String fileNodeName);
}
