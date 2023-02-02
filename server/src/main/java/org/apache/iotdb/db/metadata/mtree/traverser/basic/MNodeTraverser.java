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
package org.apache.iotdb.db.metadata.mtree.traverser.basic;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;

/**
 * This class defines any node in MTree as potential target node. On finding a path matching the
 * given pattern, if a level is specified and the path is longer than the specified level,
 * MNodeTraverser finds the node of the specified level on the path and process it. The same node
 * will not be processed more than once. If a level is not given, the current node is processed.
 */
public abstract class MNodeTraverser<R> extends Traverser<R> {

  // Level query option started from 0. For example, level of root.sg.d1.s1 is 3.
  protected int targetLevel = -1;
  protected IMNode lastVisitNode = null;

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @param store MTree store to traverse
   * @param isPrefixMatch prefix match or not
   * @throws MetadataException path does not meet the expected rules
   */
  public MNodeTraverser(
      IMNode startNode, PartialPath path, IMTreeStore store, boolean isPrefixMatch)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch);
  }

  @Override
  protected boolean acceptFullMatchedNode(IMNode node) {
    if (targetLevel >= 0) {
      if (getSizeOfAncestor() > targetLevel) {
        return getAncestorNodeByLevel(targetLevel) != lastVisitNode;
      } else if (getSizeOfAncestor() == targetLevel) {
        return node != lastVisitNode;
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  @Override
  protected boolean acceptInternalMatchedNode(IMNode node) {
    return false;
  }

  @Override
  protected boolean shouldVisitSubtreeOfFullMatchedNode(IMNode node) {
    return !node.isMeasurement();
  }

  @Override
  protected boolean shouldVisitSubtreeOfInternalMatchedNode(IMNode node) {
    return !node.isMeasurement();
  }

  public void setTargetLevel(int targetLevel) {
    this.targetLevel = targetLevel;
  }

  @Override
  protected final R generateResult(IMNode nextMatchedNode) {
    if (targetLevel >= 0) {
      if (getLevelOfNextMatchedNode() == targetLevel) {
        lastVisitNode = nextMatchedNode;
      } else {
        lastVisitNode = getAncestorNodeByLevel(targetLevel);
      }
      return transferToResult(lastVisitNode);
    } else {
      return transferToResult(nextMatchedNode);
    }
  }

  protected abstract R transferToResult(IMNode node);
}
