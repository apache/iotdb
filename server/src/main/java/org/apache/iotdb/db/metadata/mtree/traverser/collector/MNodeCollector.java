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
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;

// This class defines any node in MTree as potential target node.
public abstract class MNodeCollector<T> extends CollectorTraverser<T> {

  // traverse for specific storage group
  protected StorageGroupFilter storageGroupFilter = null;

  // level query option
  protected int targetLevel;

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
    if (targetLevel > 0) {
      if (level == targetLevel) {
        transferToResult(node);
        return true;
      }
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
