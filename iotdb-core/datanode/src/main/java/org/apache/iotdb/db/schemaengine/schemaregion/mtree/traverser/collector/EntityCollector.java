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
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.basic.EntityTraverser;

// This class defines EntityMNode as target node and defines the Entity process framework.
// TODO: set R is IDeviceSchemaInfo
public abstract class EntityCollector<R, N extends IMNode<N>> extends EntityTraverser<R, N> {

  protected EntityCollector(
      final N startNode,
      final PartialPath path,
      final IMTreeStore<N> store,
      final boolean isPrefixMatch,
      final PathPatternTree scope)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch, scope);
  }

  @Override
  protected R generateResult(final N nextMatchedNode) {
    return collectEntity(nextMatchedNode.getAsDeviceMNode());
  }

  protected abstract R collectEntity(final IDeviceMNode<N> node);
}
