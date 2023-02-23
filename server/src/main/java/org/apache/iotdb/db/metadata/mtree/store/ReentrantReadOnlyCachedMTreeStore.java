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
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.iterator.IMNodeIterator;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.File;
import java.util.Map;

public class ReentrantReadOnlyCachedMTreeStore implements IMTreeStore {
  private final CachedMTreeStore store;
  private final long readLockStamp;

  public ReentrantReadOnlyCachedMTreeStore(CachedMTreeStore store) {
    this.store = store;
    this.readLockStamp = store.stampedReadLock();
  }

  @Override
  public IMNode generatePrefix(PartialPath storageGroupPath) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public IMNode getRoot() {
    return store.getRoot();
  }

  @Override
  public boolean hasChild(IMNode parent, String name) throws MetadataException {
    return store.hasChild(parent, name, false);
  }

  @Override
  public IMNode getChild(IMNode parent, String name) throws MetadataException {
    return store.getChild(parent, name, false);
  }

  @Override
  public IMNodeIterator getChildrenIterator(IMNode parent) throws MetadataException {
    return store.getChildrenIterator(parent, false);
  }

  @Override
  public IMNodeIterator getTraverserIterator(
      IMNode parent, Map<Integer, Template> templateMap, boolean skipPreDeletedSchema)
      throws MetadataException {
    return store.getTraverserIterator(this, parent, templateMap, skipPreDeletedSchema);
  }

  @Override
  public IMNode addChild(IMNode parent, String childName, IMNode child) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void deleteChild(IMNode parent, String childName) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void updateMNode(IMNode node) {
    store.updateMNode(node, false);
  }

  @Override
  public IEntityMNode setToEntity(IMNode node) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public IMNode setToInternal(IEntityMNode entityMNode) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void setAlias(IMeasurementMNode measurementMNode, String alias) {
    throw new UnsupportedOperationException("ReadOnlyReentrantMTreeStore");
  }

  @Override
  public void pin(IMNode node) throws MetadataException {
    store.pin(node, false);
  }

  @Override
  public void unPin(IMNode node) {
    store.unPin(node, false);
  }

  @Override
  public void unPinPath(IMNode node) {
    store.unPinPath(node, false);
  }

  @Override
  public IMTreeStore getWithReentrantReadLock() {
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
