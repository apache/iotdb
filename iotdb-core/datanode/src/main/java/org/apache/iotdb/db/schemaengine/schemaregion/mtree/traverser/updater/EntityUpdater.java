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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.updater;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.basic.EntityTraverser;

public abstract class EntityUpdater<N extends IMNode<N>> extends EntityTraverser<Void, N>
    implements Updater {
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
  protected EntityUpdater(
      N startNode,
      PartialPath path,
      IMTreeStore<N> store,
      boolean isPrefixMatch,
      PathPatternTree scope)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch, scope);
  }

  @Override
  protected Void generateResult(N nextMatchedNode) {
    try {
      updateEntity(nextMatchedNode.getAsDeviceMNode());
    } catch (MetadataException e) {
      setFailure(e);
    }
    return null;
  }

  @Override
  public void update() throws MetadataException {
    while (super.hasNext()) {
      super.next();
    }
    if (!isSuccess()) {
      Throwable e = getFailure();
      throw new MetadataException(e.getMessage(), e);
    }
  }

  protected abstract void updateEntity(IDeviceMNode<N> node) throws MetadataException;
}
