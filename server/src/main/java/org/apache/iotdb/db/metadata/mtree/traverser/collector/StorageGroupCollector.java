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
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

// This class implements storage group path collection function.
public abstract class StorageGroupCollector<T> extends CollectorTraverser<T> {

  protected boolean collectInternal = false;

  public StorageGroupCollector(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    if (node.isStorageGroup()) {
      if (collectInternal) {
        collectStorageGroup(node.getAsStorageGroupMNode());
      }
      return true;
    }
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
    if (node.isStorageGroup()) {
      collectStorageGroup(node.getAsStorageGroupMNode());
      return true;
    }
    return false;
  }

  protected abstract void collectStorageGroup(IStorageGroupMNode node);

  public void setCollectInternal(boolean collectInternal) {
    this.collectInternal = collectInternal;
  }
}
