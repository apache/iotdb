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
    if(node == null){
      node = file.getChildNode(parent, name);
    }
    if(node != null){
      cacheStrategy.updateCacheStatusAfterRead(node);
    }
    return node;
  }

  @Override
  public Iterator<IMNode> getChildrenIterator(IMNode parent) {
    return file.getChildren(parent);
  }

  @Override
  public void addChild(IMNode parent, String childName, IMNode child) {
    parent.addChild(childName, child);
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
    file.close();
  }
}
