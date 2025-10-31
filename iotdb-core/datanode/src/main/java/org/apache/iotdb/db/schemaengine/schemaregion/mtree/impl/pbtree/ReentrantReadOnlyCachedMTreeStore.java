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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.io.File;
import java.util.Map;
import java.util.function.Consumer;

public class ReentrantReadOnlyCachedMTreeStore implements IMTreeStore<ICachedMNode> {
  private final CachedMTreeStore store;
  private final long readLockStamp;

  public ReentrantReadOnlyCachedMTreeStore(CachedMTreeStore store) {
    this.store = store;
    this.readLockStamp = store.stampedReadLock();
  }

  @Override
  public ICachedMNode generatePrefix(PartialPath storageGroupPath) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public ICachedMNode getRoot() {
    return store.getRoot();
  }

  @Override
  public boolean hasChild(ICachedMNode parent, String name) throws MetadataException {
    return store.hasChild(parent, name, false, true);
  }

  @Override
  public ICachedMNode getChild(ICachedMNode parent, String name) throws MetadataException {
    return store.getChild(parent, name, false, true);
  }

  @Override
  public IMNodeIterator getChildrenIterator(ICachedMNode parent) throws MetadataException {
    return store.getChildrenIterator(parent, false);
  }

  @Override
  public IMNodeIterator<ICachedMNode> getTraverserIterator(
      ICachedMNode parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
      throws MetadataException {
    return store.getTraverserIterator(this, parent, templateMap, skipPreDeletedSchema);
  }

  @Override
  public ICachedMNode addChild(ICachedMNode parent, String childName, ICachedMNode child) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void deleteChild(ICachedMNode parent, String childName) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void updateMNode(ICachedMNode node, Consumer<ICachedMNode> operation) {
    store.updateMNode(node, operation, false);
  }

  @Override
  public IDeviceMNode<ICachedMNode> setToEntity(ICachedMNode node) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public ICachedMNode setToInternal(IDeviceMNode<ICachedMNode> entityMNode) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void setAlias(IMeasurementMNode<ICachedMNode> measurementMNode, String alias) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void pin(ICachedMNode node) throws MetadataException {
    store.pin(node, false);
  }

  @Override
  public void unPin(ICachedMNode node) {
    store.unPin(node, false);
  }

  @Override
  public void unPinPath(ICachedMNode node) {
    store.unPinPath(node, false);
  }

  @Override
  public IMTreeStore<ICachedMNode> getWithReentrantReadLock() {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public ReleaseFlushMonitor.RecordNode recordTraverserStatistics() {
    return store.recordTraverserStatistics();
  }

  @Override
  public void recordTraverserMetric(long costTime) {
    store.recordTraverserMetric(costTime);
  }

  public void unlockRead() {
    store.stampedReadUnlock(readLockStamp);
  }
}
