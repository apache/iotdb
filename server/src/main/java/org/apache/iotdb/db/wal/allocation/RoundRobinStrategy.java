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

import org.apache.iotdb.db.wal.node.IWALNode;
import org.apache.iotdb.db.wal.node.WALNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This strategy creates n wal nodes and allocate them by round-robin strategy. In other words,
 * several identifiers (like data regions) can share one wal node.
 */
public class RoundRobinStrategy extends AbstractNodeAllocationStrategy {
  /** max wal nodes number */
  private final int maxWalNodeNum;
  /** protect concurrent safety of wal nodes, including walNodes, nodeCursor and nodeIdCounter */
  private final Lock nodesLock = new ReentrantLock();
  // region these variables should be protected by nodesLock
  /** wal nodes, the max number of wal nodes is MAX_WAL_NUM */
  private final List<WALNode> walNodes;
  /** help allocate node for users */
  private int nodeCursor = -1;
  /** each wal node has a unique int value identifier */
  private int nodeIdCounter = -1;
  // endregion

  public RoundRobinStrategy(int maxWalNodeNum) {
    this.maxWalNodeNum = maxWalNodeNum;
    this.walNodes = new ArrayList<>(maxWalNodeNum);
  }

  @Override
  public IWALNode applyForWALNode(String applicantUniqueId) {
    WALNode selectedNode;
    nodesLock.lock();
    try {
      if (walNodes.size() < maxWalNodeNum) {
        nodeIdCounter++;
        IWALNode node = createWALNode(String.valueOf(nodeIdCounter));
        if (!(node instanceof WALNode)) {
          return node;
        }
        selectedNode = (WALNode) node;
        walNodes.add(selectedNode);
      } else {
        // select next wal node by sequence order
        nodeCursor = (nodeCursor + 1) % maxWalNodeNum;
        selectedNode = walNodes.get(nodeCursor);
      }
    } finally {
      nodesLock.unlock();
    }
    return selectedNode;
  }

  @Override
  public List<WALNode> getNodesSnapshot() {
    List<WALNode> snapshot;
    if (walNodes.size() < maxWalNodeNum) {
      nodesLock.lock();
      try {
        snapshot = new ArrayList<>(walNodes);
      } finally {
        nodesLock.unlock();
      }
    } else {
      snapshot = walNodes;
    }
    return snapshot;
  }

  /** non-thread-safe, used for metrics only */
  @Override
  public int getNodesNum() {
    return walNodes.size();
  }

  @Override
  public void clear() {
    nodesLock.lock();
    try {
      nodeCursor = -1;
      nodeIdCounter = -1;
      for (WALNode walNode : walNodes) {
        walNode.close();
      }
      walNodes.clear();
    } finally {
      nodesLock.unlock();
    }
  }
}
