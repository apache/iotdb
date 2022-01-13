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

import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.ICacheStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.IMemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.MemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.file.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.file.MockSchemaFile;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CachedMTreeStore implements IMTreeStore {

  private IMemManager memManager = new MemManager();

  private ICacheStrategy cacheStrategy = new CacheStrategy();

  private ISchemaFile file = new MockSchemaFile();

  private InternalMNode root;

  @Override
  public void init() throws IOException {}

  @Override
  public IMNode getRoot() {
    return root;
  }

  @Override
  public boolean hasChild(IMNode parent, String name) {
    return getChild(parent, name) == null;
  }

  @Override
  public IMNode getChild(IMNode parent, String name) {
    IMNode node = parent.getChild(name);
    if (node == null) {
      node = file.getChildNode(parent, name);
      if (node != null) {
        cacheMNodeInMemory(node);
      }
    }
    if (node != null) {
      cacheStrategy.updateCacheStatusAfterRead(node);
    }
    return node;
  }

  @Override
  public Iterator<IMNode> getChildrenIterator(IMNode parent) {
    return new CachedMNodeIterator(file.getChildren(parent));
  }

  @Override
  public void addChild(IMNode parent, String childName, IMNode child) {
    parent.addChild(childName, child);
    cacheMNodeInMemory(child);
    cacheStrategy.updateCacheStatusAfterAppend(child);
  }

  @Override
  public void addAlias(IEntityMNode parent, String alias, IMeasurementMNode child) {
    parent.addAlias(alias, child);
  }

  @Override
  public void deleteChild(IMNode parent, String childName) {
    IMNode node = parent.getChild(childName);
    parent.deleteChild(childName);
    cacheStrategy.remove(node);
    file.deleteMNode(node);
  }

  @Override
  public void deleteAliasChild(IEntityMNode parent, String alias) {
    parent.deleteAliasChild(alias);
  }

  @Override
  public void updateMNode(IMNode node) {
    cacheStrategy.updateCacheStatusAfterUpdate(node);
  }

  @Override
  public void createSnapshot() throws IOException {}

  @Override
  public void clear() {
    root = null;
    cacheStrategy.clear();
    memManager.clear();
    file.close();
  }

  private void cacheMNodeInMemory(IMNode node) {
    if (!memManager.requestMemResource(node)) {
      executeMemoryRelease();
      memManager.requestMemResource(node);
    }
  }

  private void executeMemoryRelease() {
    List<IMNode> nodesToPersist = cacheStrategy.collectVolatileMNodes(root);
    for (IMNode volatileNode : nodesToPersist) {
      file.writeMNode(volatileNode);
      cacheStrategy.updateCacheStatusAfterPersist(volatileNode);
    }
    List<IMNode> evictedMNodes;
    while (!memManager.isUnderThreshold()) {
      evictedMNodes = cacheStrategy.evict();
      for (IMNode evictedMNode : evictedMNodes) {
        memManager.releaseMemResource(evictedMNode);
      }
    }
  }

  private class CachedMNodeIterator implements Iterator<IMNode> {

    Iterator<IMNode> diskIterator;

    CachedMNodeIterator(Iterator<IMNode> diskIterator) {
      this.diskIterator = diskIterator;
    }

    @Override
    public boolean hasNext() {
      return diskIterator.hasNext();
    }

    @Override
    public IMNode next() {
      IMNode result = diskIterator.next();
      if (result != null) {
        cacheMNodeInMemory(result);
        cacheStrategy.updateCacheStatusAfterRead(result);
        return result;
      }
      throw new NoSuchElementException();
    }
  }
}
