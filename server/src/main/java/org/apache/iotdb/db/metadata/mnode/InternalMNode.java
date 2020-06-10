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
package org.apache.iotdb.db.metadata.mnode;

import java.util.LinkedHashMap;
import org.apache.iotdb.db.exception.metadata.DeleteFailedException;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

public class InternalMNode extends MNode {

  private static final long serialVersionUID = 7999036474525817732L;

  private Map<String, MNode> children;
  private Map<String, MNode> aliasChildren;

  protected ReadWriteLock lock = new ReentrantReadWriteLock();

  public InternalMNode(MNode parent, String name) {
    super(parent, name);
    this.children = new LinkedHashMap<>();
    this.aliasChildren = new LinkedHashMap<>();
  }

  @Override
  public boolean hasChild(String name) {
    return this.children.containsKey(name) || this.aliasChildren.containsKey(name);
  }

  @Override
  public void addChild(String name, MNode child) {
    children.put(name, child);
  }


  /**
   * If delete a leafMNode, lock its parent, if delete an InternalNode, lock itself
   */
  @Override
  public void deleteChild(String name) throws DeleteFailedException {
    if (children.containsKey(name)) {
      Lock writeLock;
      // if its child node is leaf node, we need to acquire the write lock of the current device node
      if (children.get(name) instanceof MeasurementMNode) {
        writeLock = lock.writeLock();
      } else {
        // otherwise, we only need to acquire the write lock of its child node.
        writeLock = ((InternalMNode) children.get(name)).lock.writeLock();
      }
      if (writeLock.tryLock()) {
        children.remove(name);
        writeLock.unlock();
      } else {
        throw new DeleteFailedException(getFullPath() + PATH_SEPARATOR + name);
      }
    }
}

  @Override
  public void deleteAliasChild(String alias) throws DeleteFailedException {

    if (lock.writeLock().tryLock()) {
      aliasChildren.remove(alias);
      lock.writeLock().unlock();
    } else {
      throw new DeleteFailedException(getFullPath() + PATH_SEPARATOR + alias);
    }
  }

  @Override
  public MNode getChild(String name) {
    return children.containsKey(name) ? children.get(name) : aliasChildren.get(name);
  }

  @Override
  public int getLeafCount() {
    int leafCount = 0;
    for (MNode child : this.children.values()) {
      leafCount += child.getLeafCount();
    }
    return leafCount;
  }

  @Override
  public void addAlias(String alias, MNode child) {
    aliasChildren.put(alias, child);
  }

  @Override
  public Map<String, MNode> getChildren() {
    return children;
  }

  public void readLock() {
    InternalMNode node = this;
    while (node != null) {
      node.lock.readLock().lock();
      node = (InternalMNode) node.parent;
    }
  }

  public void readUnlock() {
    InternalMNode node = this;
    while (node != null) {
      node.lock.readLock().unlock();
      node = (InternalMNode) node.parent;
    }
  }
}