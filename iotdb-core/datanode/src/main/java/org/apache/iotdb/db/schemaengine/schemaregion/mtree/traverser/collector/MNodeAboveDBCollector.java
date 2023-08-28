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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.collector;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;

import java.util.HashSet;
import java.util.Set;

public abstract class MNodeAboveDBCollector<T, N extends IMNode<N>> extends MNodeCollector<T, N> {

  protected Set<PartialPath> involvedDatabaseMNodes = new HashSet<>();

  protected MNodeAboveDBCollector(
      N startNode, PartialPath path, IMTreeStore<N> store, boolean isPrefixMatch)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch);
  }

  @Override
  protected boolean shouldVisitSubtreeOfFullMatchedNode(N node) {
    if (node.isDatabase()) {
      involvedDatabaseMNodes.add(getParentPartialPath().concatNode(node.getName()));
      return false;
    } else {
      return super.shouldVisitSubtreeOfFullMatchedNode(node);
    }
  }

  @Override
  protected boolean shouldVisitSubtreeOfInternalMatchedNode(N node) {
    if (node.isDatabase()) {
      involvedDatabaseMNodes.add(getParentPartialPath().concatNode(node.getName()));
      return false;
    } else {
      return super.shouldVisitSubtreeOfInternalMatchedNode(node);
    }
  }

  public Set<PartialPath> getInvolvedDatabaseMNodes() {
    return involvedDatabaseMNodes;
  }
}
