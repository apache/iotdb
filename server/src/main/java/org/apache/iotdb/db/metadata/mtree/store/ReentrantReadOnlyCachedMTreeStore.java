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
package org.apache.iotdb.db.metadata.mtree.store;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.db.metadata.mnode.schemafile.ICacheMNode;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.File;
import java.util.Map;

public class ReentrantReadOnlyCachedMTreeStore implements IMTreeStore<ICacheMNode> {
  private final CachedMTreeStore store;
  private final long readLockStamp;

  public ReentrantReadOnlyCachedMTreeStore(CachedMTreeStore store) {
    this.store = store;
    this.readLockStamp = store.stampedReadLock();
  }

  @Override
  public ICacheMNode generatePrefix(PartialPath storageGroupPath) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public ICacheMNode getRoot() {
    return store.getRoot();
  }

  @Override
  public boolean hasChild(ICacheMNode parent, String name) throws MetadataException {
    return store.hasChild(parent, name, false);
  }

  @Override
  public ICacheMNode getChild(ICacheMNode parent, String name) throws MetadataException {
    return store.getChild(parent, name, false);
  }

  @Override
  public IMNodeIterator getChildrenIterator(ICacheMNode parent) throws MetadataException {
    return store.getChildrenIterator(parent, false);
  }

  @Override
  public IMNodeIterator<ICacheMNode> getTraverserIterator(
      ICacheMNode parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
      throws MetadataException {
    return store.getTraverserIterator(this, parent, templateMap, skipPreDeletedSchema);
  }

  @Override
  public ICacheMNode addChild(ICacheMNode parent, String childName, ICacheMNode child) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void deleteChild(ICacheMNode parent, String childName) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void updateMNode(ICacheMNode node) {
    store.updateMNode(node, false);
  }

  @Override
  public IDeviceMNode<ICacheMNode> setToEntity(ICacheMNode node) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public ICacheMNode setToInternal(IDeviceMNode<ICacheMNode> entityMNode) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void setAlias(IMeasurementMNode<ICacheMNode> measurementMNode, String alias) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void pin(ICacheMNode node) throws MetadataException {
    store.pin(node, false);
  }

  @Override
  public void unPin(ICacheMNode node) {
    store.unPin(node, false);
  }

  @Override
  public void unPinPath(ICacheMNode node) {
    store.unPinPath(node, false);
  }

  @Override
  public IMTreeStore<ICacheMNode> getWithReentrantReadLock() {
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

  public void unlockRead() {
    store.stampedReadUnlock(readLockStamp);
  }
}
