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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.impl;

import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.MeasurementInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.cache.CacheEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.basic.CachedBasicMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.CachedMNodeContainer;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class CachedMeasurementMNode extends AbstractMeasurementMNode<ICachedMNode, CachedBasicMNode>
    implements ICachedMNode {

  public CachedMeasurementMNode(
      IDeviceMNode<ICachedMNode> parent, String name, IMeasurementSchema schema, String alias) {
    super(
        new CachedBasicMNode(parent == null ? null : parent.getAsMNode(), name),
        new MeasurementInfo(schema, alias));
  }

  @Override
  public CacheEntry getCacheEntry() {
    return basicMNode.getCacheEntry();
  }

  @Override
  public void setCacheEntry(CacheEntry cacheEntry) {
    basicMNode.setCacheEntry(cacheEntry);
  }

  @Override
  public ICachedMNode getAsMNode() {
    return this;
  }

  @Override
  public IMNodeContainer<ICachedMNode> getChildren() {
    return CachedMNodeContainer.emptyMNodeContainer();
  }

  @Override
  public final boolean isLogicalView() {
    return false;
  }
}
