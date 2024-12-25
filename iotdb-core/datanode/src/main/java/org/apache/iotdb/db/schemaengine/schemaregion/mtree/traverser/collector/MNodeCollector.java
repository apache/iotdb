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
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.basic.MNodeTraverser;

/**
 * This class defines any node in MTree as potential target node. On finding a path matching the
 * given pattern, if a level is specified and the path is longer than the specified level,
 * MNodeLevelCounter finds the node of the specified level on the path and process it. The same node
 * will not be processed more than once. If a level is not given, the current node is processed.
 */
// TODO: set R to IMNodeInfo
public abstract class MNodeCollector<R, N extends IMNode<N>> extends MNodeTraverser<R, N> {

  protected MNodeCollector(
      final N startNode,
      final PartialPath path,
      final IMTreeStore<N> store,
      final boolean isPrefixMatch,
      final PathPatternTree scope)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch, scope);
  }

  protected final R transferToResult(final N node) {
    return collectMNode(node);
  }

  protected abstract R collectMNode(final N node);
}
