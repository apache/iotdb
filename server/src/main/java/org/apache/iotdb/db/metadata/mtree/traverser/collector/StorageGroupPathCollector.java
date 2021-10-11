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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.LinkedList;
import java.util.List;

// This class implements storage group path collection function.
public class StorageGroupPathCollector extends CollectorTraverser<List<PartialPath>> {

  protected boolean collectInternal = false;

  public StorageGroupPathCollector(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
    this.resultSet = new LinkedList<>();
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    if (node.isStorageGroup()) {
      if (collectInternal) {
        transferToResult(node);
      }
      return true;
    }
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
    if (node.isStorageGroup()) {
      transferToResult(node);
      return true;
    }
    return false;
  }

  private void transferToResult(IMNode node) {
    resultSet.add(node.getPartialPath());
  }

  public void setCollectInternal(boolean collectInternal) {
    this.collectInternal = collectInternal;
  }
}
