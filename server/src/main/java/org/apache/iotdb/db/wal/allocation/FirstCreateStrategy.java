/*
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
package org.apache.iotdb.db.wal.allocation;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.iot.wal.ConsensusReqReader;
import org.apache.iotdb.db.wal.node.IWALNode;
import org.apache.iotdb.db.wal.node.WALNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This strategy creates one wal node for each unique identifier. In other words, each identifier
 * (like data region) has its own wal node.
 */
public class FirstCreateStrategy extends AbstractNodeAllocationStrategy {
  /** protect concurrent safety of wal nodes, including walNodes, nodeCursor and nodeIdCounter */
  private final Lock nodesLock = new ReentrantLock();
  // region these variables should be protected by nodesLock
  /** wal nodes */
  private final Map<String, WALNode> identifier2Nodes = new HashMap<>();
  // endregion

  @Override
  public IWALNode applyForWALNode(String applicantUniqueId) {
    nodesLock.lock();
    try {
      if (identifier2Nodes.containsKey(applicantUniqueId)) {
        return identifier2Nodes.get(applicantUniqueId);
      }

      IWALNode walNode = createWALNode(applicantUniqueId);
      if (walNode instanceof WALNode) {
        // avoid deletion
        walNode.setSafelyDeletedSearchIndex(ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX);
        identifier2Nodes.put(applicantUniqueId, (WALNode) walNode);
      }
      return walNode;
    } finally {
      nodesLock.unlock();
    }
  }

  public void registerWALNode(
      String applicantUniqueId, String logDirectory, long startFileVersion, long startSearchIndex) {
    nodesLock.lock();
    try {
      if (identifier2Nodes.containsKey(applicantUniqueId)) {
        return;
      }

      IWALNode walNode =
          createWALNode(applicantUniqueId, logDirectory, startFileVersion, startSearchIndex);
      if (walNode instanceof WALNode) {
        // avoid deletion
        walNode.setSafelyDeletedSearchIndex(ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX);
        identifier2Nodes.put(applicantUniqueId, (WALNode) walNode);
      }
    } finally {
      nodesLock.unlock();
    }
  }

  public void deleteWALNode(String applicantUniqueId) {
    nodesLock.lock();
    try {
      WALNode walNode = identifier2Nodes.remove(applicantUniqueId);
      if (walNode != null) {
        walNode.close();
        if (walNode.getLogDirectory().exists()) {
          FileUtils.deleteDirectory(walNode.getLogDirectory());
        }
      }
    } finally {
      nodesLock.unlock();
    }
  }

  @Override
  public List<WALNode> getNodesSnapshot() {
    List<WALNode> snapshot;
    nodesLock.lock();
    try {
      snapshot = new ArrayList<>(identifier2Nodes.values());
    } finally {
      nodesLock.unlock();
    }
    return snapshot;
  }

  @Override
  public void clear() {
    nodesLock.lock();
    try {
      for (WALNode walNode : identifier2Nodes.values()) {
        walNode.close();
      }
      identifier2Nodes.clear();
    } finally {
      nodesLock.unlock();
    }
  }
}
