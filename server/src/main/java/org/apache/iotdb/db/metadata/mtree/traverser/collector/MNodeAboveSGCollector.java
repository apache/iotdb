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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;

import java.util.HashSet;
import java.util.Set;

public abstract class MNodeAboveSGCollector<T> extends MNodeCollector<T> {

  protected Set<PartialPath> involvedStorageGroupMNodes = new HashSet<>();

  public MNodeAboveSGCollector(IMNode startNode, PartialPath path, IMTreeStore store)
      throws MetadataException {
    super(startNode, path, store);
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    boolean shouldSkipSubtree = super.processInternalMatchedMNode(node, idx, level);
    if (node.isStorageGroup()) {
      involvedStorageGroupMNodes.add(node.getPartialPath());
      return true;
    }
    return shouldSkipSubtree;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
    boolean shouldSkipSubtree = super.processFullMatchedMNode(node, idx, level);
    if (node.isStorageGroup()) {
      involvedStorageGroupMNodes.add(node.getPartialPath());
      return true;
    }
    return shouldSkipSubtree;
  }

  public Set<PartialPath> getInvolvedStorageGroupMNodes() {
    return involvedStorageGroupMNodes;
  }
}
