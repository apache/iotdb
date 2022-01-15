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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.HashSet;
import java.util.Set;

// This node implements node count function.
public class MNodeLevelCounter extends CounterTraverser {

  // level query option
  protected int targetLevel;

  private Set<IMNode> processedNodes = new HashSet<>();

  public MNodeLevelCounter(IMNode startNode, PartialPath path, int targetLevel)
      throws MetadataException {
    super(startNode, path);
    this.targetLevel = targetLevel;
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
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
      count++;
    }
    return true;
  }
}
