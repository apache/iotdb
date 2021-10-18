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

import java.util.Set;
import java.util.TreeSet;

// This class implements the EntityMNode path collection function.
// Compared with BelongedEntityPathCollector, this class only process entities that full match the
// path
// pattern.
public class EntityPathCollector extends CollectorTraverser<Set<PartialPath>> {

  public EntityPathCollector(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
    this.resultSet = new TreeSet<>();
  }

  public EntityPathCollector(IMNode startNode, PartialPath path, int limit, int offset)
      throws MetadataException {
    super(startNode, path, limit, offset);
    this.resultSet = new TreeSet<>();
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level) {
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level) {
    if (node.isEntity()) {
      if (hasLimit) {
        curOffset += 1;
        if (curOffset < offset) {
          return true;
        }
      }
      resultSet.add(node.getPartialPath());
      if (hasLimit) {
        count += 1;
      }
    }
    return false;
  }
}
