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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.iterator;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.iterator.AbstractTraverserIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.Map;

public class CachedTraverserIterator extends AbstractTraverserIterator<ICachedMNode> {
  private final IMTreeStore<ICachedMNode> store;

  public CachedTraverserIterator(
      IMTreeStore<ICachedMNode> store,
      IDeviceMNode<ICachedMNode> parent,
      Map<Integer, Template> templateMap,
      IMNodeFactory<ICachedMNode> nodeFactory)
      throws MetadataException {
    super(store, parent, templateMap, nodeFactory);
    this.store = store;
  }

  @Override
  public void close() {
    if (nextMatchedNode != null && usingDirectChildrenIterator) {
      store.unPin(nextMatchedNode);
    }
    super.close();
  }
}
