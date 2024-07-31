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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.basic;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.Traverser;

public abstract class EntityTraverser<R, N extends IMNode<N>> extends Traverser<R, N> {

  private boolean usingTemplate = false;
  private int schemaTemplateId = -1;

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @param store MTree store to traverse
   * @param isPrefixMatch prefix match or not
   * @param scope traversing scope
   * @throws MetadataException path does not meet the expected rules
   */
  protected EntityTraverser(
      final N startNode,
      final PartialPath path,
      final IMTreeStore<N> store,
      final boolean isPrefixMatch,
      final PathPatternTree scope)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch, scope);
  }

  @Override
  protected boolean mayTargetNodeType(final N node) {
    if (node.isDevice()) {
      return (!usingTemplate || schemaTemplateId == node.getAsDeviceMNode().getSchemaTemplateId());
    }
    return false;
  }

  @Override
  protected boolean acceptFullMatchedNode(final N node) {
    if (node.isDevice()) {
      return (!usingTemplate || schemaTemplateId == node.getAsDeviceMNode().getSchemaTemplateId());
    }
    return false;
  }

  @Override
  protected boolean acceptInternalMatchedNode(final N node) {
    return false;
  }

  @Override
  protected boolean shouldVisitSubtreeOfFullMatchedNode(final N node) {
    return !node.isMeasurement();
  }

  @Override
  protected boolean shouldVisitSubtreeOfInternalMatchedNode(final N node) {
    return !node.isMeasurement();
  }

  public void setSchemaTemplateFilter(final int schemaTemplateId) {
    this.usingTemplate = true;
    this.schemaTemplateId = schemaTemplateId;
  }
}
