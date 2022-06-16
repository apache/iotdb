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
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;

import java.util.HashMap;
import java.util.Map;

public class MeasurementGroupByLevelCounter extends Traverser {

  // level query option
  private int groupByLevel;

  private Map<PartialPath, Integer> result = new HashMap<>();

  // path representing current branch and matching the pattern and level
  private PartialPath path;

  public MeasurementGroupByLevelCounter(
      IMNode startNode, PartialPath path, IMTreeStore store, int groupByLevel)
      throws MetadataException {
    super(startNode, path, store);
    this.groupByLevel = groupByLevel;
    checkLevelAboveSG();
  }

  /**
   * The traverser may start traversing from a storageGroupMNode, which is an InternalMNode of the
   * whole MTree.
   */
  private void checkLevelAboveSG() {
    if (groupByLevel >= startLevel) {
      return;
    }
    IMNode parent = startNode.getParent();
    int level = startLevel;
    while (parent != null) {
      level--;
      if (level == groupByLevel) {
        path = parent.getPartialPath();
        result.putIfAbsent(path, 0);
        break;
      }
      parent = parent.getParent();
    }
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException {
    if (level == groupByLevel) {
      path = node.getPartialPath();
      result.putIfAbsent(path, 0);
    }
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException {
    if (level == groupByLevel) {
      path = node.getPartialPath();
      result.putIfAbsent(path, 0);
    }
    if (!node.isMeasurement()) {
      return false;
    }
    if (level >= groupByLevel) {
      result.put(path, result.get(path) + 1);
    }
    return true;
  }

  public Map<PartialPath, Integer> getResult() {
    return result;
  }

  public void setResult(Map<PartialPath, Integer> result) {
    this.result = result;
  }
}
