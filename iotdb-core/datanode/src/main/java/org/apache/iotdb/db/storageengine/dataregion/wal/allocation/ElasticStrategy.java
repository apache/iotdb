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

package org.apache.iotdb.db.storageengine.dataregion.wal.allocation;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This strategy creates wal nodes according to the number of memTables. Each wal node manages fixed
 * number of memTables.
 */
public class ElasticStrategy extends AbstractNodeAllocationStrategy {
  // each wal node manages fixed number of memTables
  public static final int APPLICATION_NODE_RATIO = 4;

  // protect concurrent safety of wal nodes, including walNodes, nodeCursor and nodeIdCounter
  private final Lock nodesLock = new ReentrantLock();
  // region these variables should be protected by nodesLock
  // wal nodes, the max number of wal nodes is MAX_WAL_NUM
  private final List<WALNode> walNodes;
  // help allocate node for users
  private final Map<String, WALNode> uniqueId2Nodes = new HashMap<>();
  // each wal node has a unique long value identifier
  private int nodeIdCounter = -1;

  // endregion

  public ElasticStrategy() {
    this.walNodes = new ArrayList<>();
  }

  // it's safe to not close WALNode here, we use clear method to close all WALNodes.
  @SuppressWarnings("squid:S2095")
  @Override
  public IWALNode applyForWALNode(String applicantUniqueId) {
    nodesLock.lock();
    try {
      if (!uniqueId2Nodes.containsKey(applicantUniqueId)) {
        // add 1 node when reaching threshold
        if (uniqueId2Nodes.size() == walNodes.size() * APPLICATION_NODE_RATIO) {
          nodeIdCounter++;
          IWALNode node = createWALNode(String.valueOf(nodeIdCounter));
          if (!(node instanceof WALNode)) {
            return node;
          }
          walNodes.add((WALNode) node);
        }
        uniqueId2Nodes.put(applicantUniqueId, walNodes.get(walNodes.size() - 1));
      }

      return uniqueId2Nodes.get(applicantUniqueId);
    } finally {
      nodesLock.unlock();
    }
  }

  public void deleteUniqueIdAndMayDeleteWALNode(String applicantUniqueId) {
    nodesLock.lock();
    try {
      WALNode walNode = uniqueId2Nodes.remove(applicantUniqueId);
      if (!uniqueId2Nodes.containsValue(walNode)) {
        if (walNode != null) {
          walNode.close();
          if (walNode.getLogDirectory().exists()) {
            FileUtils.deleteFileOrDirectory(walNode.getLogDirectory());
          }
          WALManager.getInstance().subtractTotalDiskUsage(walNode.getDiskUsage());
          WALManager.getInstance().subtractTotalFileNum(walNode.getFileNum());
        }
        walNodes.remove(walNode);
        if (walNodes.isEmpty()) {
          nodeIdCounter = -1;
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
      snapshot = new ArrayList<>(walNodes);
    } finally {
      nodesLock.unlock();
    }
    return snapshot;
  }

  @Override
  public int getNodesNum() {
    return walNodes.size();
  }

  @Override
  public void clear() {
    nodesLock.lock();
    try {
      nodeIdCounter = -1;
      uniqueId2Nodes.clear();
      for (WALNode walNode : walNodes) {
        walNode.close();
      }
      walNodes.clear();
    } finally {
      nodesLock.unlock();
    }
  }
}
