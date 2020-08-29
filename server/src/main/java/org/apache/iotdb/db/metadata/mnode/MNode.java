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

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.DeleteFailedException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.rescon.CachedStringPool;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class MNode implements Serializable {

  private static final long serialVersionUID = -770028375899514063L;

  /**
   * Name of the MNode
   */
  protected String name;

  protected MNode parent;

  private static Map<String, String> cachedPathPool = CachedStringPool.getInstance().getCachedPool();

  /**
   * from root to this node, only be set when used once for InternalMNode
   */
  protected String fullPath;

  transient Map<String, MNode> children = null;
  private transient Map<String, MNode> aliasChildren = null;

  protected transient ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Constructor of MNode.
   */
  public MNode(MNode parent, String name) {
    this.parent = parent;
    this.name = name;
  }

  /**
   * check whether the MNode has a child with the name
   */
  public boolean hasChild(String name) {
    return (children != null && children.containsKey(name)) ||
        (aliasChildren != null && aliasChildren.containsKey(name));
  }

  /**
   * node key, name or alias
   */
  public void addChild(String name, MNode child) {
    if (children == null) {
      children = new LinkedHashMap<>();
    }
    children.put(name, child);
  }

  /**
   * delete a child
   */
  public void deleteChild(String name) throws DeleteFailedException {
    if (children != null && children.containsKey(name)) {
      // acquire the write lock of its child node.
      Lock writeLock = (children.get(name)).lock.writeLock();
      if (writeLock.tryLock()) {
        children.remove(name);
        writeLock.unlock();
      } else {
        throw new DeleteFailedException(getFullPath() + PATH_SEPARATOR + name);
      }
    }
  }

  /**
   * delete the alias of a child
   */
  public void deleteAliasChild(String alias) throws DeleteFailedException {
    if (aliasChildren == null) {
      return;
    }
    if (lock.writeLock().tryLock()) {
      aliasChildren.remove(alias);
      lock.writeLock().unlock();
    } else {
      throw new DeleteFailedException(getFullPath() + PATH_SEPARATOR + alias);
    }
  }

  /**
   * get the child with the name
   */
  public MNode getChild(String name) {
    MNode child = null;
    if (children != null) {
      child = children.get(name);
    }
    if (child != null) {
      return child;
    }
    return aliasChildren == null ? null : aliasChildren.get(name);
  }

  /**
   * get the count of all leaves whose ancestor is current node
   */
  public int getLeafCount() {
    if (children == null) {
      return 0;
    }
    int leafCount = 0;
    for (MNode child : children.values()) {
      leafCount += child.getLeafCount();
    }
    return leafCount;
  }

  /**
   * add an alias
   */
  public void addAlias(String alias, MNode child) {
    if (aliasChildren == null) {
      aliasChildren = new LinkedHashMap<>();
    }
    aliasChildren.put(alias, child);
  }

  /**
   * get full path
   */
  public String getFullPath() {
    if (fullPath == null) {
      fullPath = concatFullPath();
      String cachedFullPath = cachedPathPool.get(fullPath);
      if (cachedFullPath == null) {
        cachedPathPool.put(fullPath, fullPath);
      } else {
        fullPath = cachedFullPath;
      }
    }
    return fullPath;
  }

  public PartialPath getPartialPath() {
    List<String> detachedPath = new ArrayList<>();
    MNode temp = this;
    detachedPath.add(temp.getName());
    while (temp.getParent() != null) {
      temp = temp.getParent();
      detachedPath.add(0, temp.getName());
    }
    return new PartialPath(detachedPath.toArray(new String[0]));
  }

  String concatFullPath() {
    StringBuilder builder = new StringBuilder(name);
    MNode curr = this;
    while (curr.getParent() != null) {
      curr = curr.getParent();
      builder.insert(0, IoTDBConstant.PATH_SEPARATOR).insert(0, curr.name);
    }
    return builder.toString();
  }

  @Override
  public String toString() {
    return this.getName();
  }

  public MNode getParent() {
    return parent;
  }

  public void setParent(MNode parent) {
    this.parent = parent;
  }

  public Map<String, MNode> getChildren() {
    if (children == null) {
      return new LinkedHashMap<>();
    }
    return children;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setChildren(Map<String, MNode> children) {
    this.children = children;
  }

  public void serializeTo(BufferedWriter bw) throws IOException {
    serializeChildren(bw);

    String s = String.valueOf(MetadataConstant.MNODE_TYPE) + "," + name + ","
        + (children == null ? "0" : children.size());
    bw.write(s);
    bw.newLine();
  }

  void serializeChildren(BufferedWriter bw) throws IOException {
    if (children == null) {
      return;
    }
    for (Entry<String, MNode> entry : children.entrySet()) {
      entry.getValue().serializeTo(bw);
    }
  }

  public void readLock() {
    MNode node = this;
    while (node != null) {
      node.lock.readLock().lock();
      node = node.parent;
    }
  }

  public void readUnlock() {
    MNode node = this;
    while (node != null) {
      node.lock.readLock().unlock();
      node = node.parent;
    }
  }
}
