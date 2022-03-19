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
package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.SchemaEngine.StorageGroupFilter;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.HashSet;
import java.util.Set;

/**
 * This class defines any node in MTree as potential target node. On finding a path matching the
 * given pattern, if a level is specified and the path is longer than the specified level,
 * MNodeLevelCounter finds the node of the specified level on the path and process it. The same node
 * will not be processed more than once. If a level is not given, the current node is processed.
 */
public abstract class MNodeCollector<T> extends CollectorTraverser<T> {

  // traverse for specific storage group
  protected StorageGroupFilter storageGroupFilter = null;

  // level query option
  protected int targetLevel = -1;

  private Set<IMNode> processedNodes = new HashSet<>();

  public MNodeCollector(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
  }

  @Override
  protected void traverse(IMNode node, int idx, int level) throws MetadataException {
    if (storageGroupFilter != null
        && node.isStorageGroup()
        && !storageGroupFilter.satisfy(node.getFullPath())) {
      return;
    }
    super.traverse(node, idx, level);
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
    if (targetLevel >= 0) {
      // move the cursor the given level when matched
      if (level < targetLevel) {
        return false;
      }
      while (level > targetLevel) {
        node = node.getParent();
        level--;
      }
      // record processed node so they will not be processed twice
      if (!processedNodes.contains(node)) {
        processedNodes.add(node);
        transferToResult(node);
      }
      return true;
    } else {
      transferToResult(node);
    }
    return false;
  }

  protected abstract void transferToResult(IMNode node);

  public void setStorageGroupFilter(StorageGroupFilter storageGroupFilter) {
    this.storageGroupFilter = storageGroupFilter;
  }

  public void setTargetLevel(int targetLevel) {
    this.targetLevel = targetLevel;
  }
}
