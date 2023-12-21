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

import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseMNode;
import org.apache.iotdb.commons.schema.node.info.IDeviceInfo;
import org.apache.iotdb.commons.schema.node.role.IInternalMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.DatabaseInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.cache.CacheEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

public class CachedDatabaseMNode
    extends AbstractDatabaseMNode<ICachedMNode, CachedBasicInternalMNode>
    implements ICachedMNode, IInternalMNode<ICachedMNode> {

  public CachedDatabaseMNode(ICachedMNode parent, String name) {
    super(new CachedBasicInternalMNode(parent, name), new DatabaseInfo<>());
  }

  // TODO: @yukun, remove this constructor
  public CachedDatabaseMNode(ICachedMNode parent, String name, long dataTTL) {
    this(parent, name);
    setDataTTL(dataTTL);
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
  public LockEntry getLockEntry() {
    return basicMNode.getLockEntry();
  }

  @Override
  public void setLockEntry(LockEntry lockEntry) {
    basicMNode.setLockEntry(lockEntry);
  }

  @Override
  public ICachedMNode getAsMNode() {
    return this;
  }

  @Override
  public IDeviceInfo<ICachedMNode> getDeviceInfo() {
    return basicMNode.getDeviceInfo();
  }

  @Override
  public void setDeviceInfo(IDeviceInfo<ICachedMNode> deviceInfo) {
    basicMNode.setDeviceInfo(deviceInfo);
  }
}
