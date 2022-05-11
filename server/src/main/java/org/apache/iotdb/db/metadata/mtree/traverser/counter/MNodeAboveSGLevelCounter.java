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
package org.apache.iotdb.db.metadata.mtree.traverser.counter;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;

import java.util.HashSet;
import java.util.Set;

public class MNodeAboveSGLevelCounter extends MNodeLevelCounter {

  protected Set<PartialPath> involvedStorageGroupMNodes = new HashSet<>();

  public MNodeAboveSGLevelCounter(
      IMNode startNode, PartialPath path, IMTreeStore store, int targetLevel)
      throws MetadataException {
    super(startNode, path, store, targetLevel);
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    if (node.isStorageGroup()) {
      involvedStorageGroupMNodes.add(node.getPartialPath());
      return true;
    }
    return super.processInternalMatchedMNode(node, idx, level);
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
    if (node.isStorageGroup()) {
      involvedStorageGroupMNodes.add(node.getPartialPath());
      return true;
    }
    return super.processFullMatchedMNode(node, idx, level);
  }

  public Set<PartialPath> getInvolvedStorageGroupMNodes() {
    return involvedStorageGroupMNodes;
  }
}
