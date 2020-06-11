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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.DeleteFailedException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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

  /**
   * from root to this node, only be set when used once for InternalMNode
   */
  protected String fullPath;

  private Map<String, MNode> children;
  private Map<String, MNode> aliasChildren;

  protected ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Constructor of MNode.
   */
  public MNode(MNode parent, String name) {
    this.parent = parent;
    this.name = name;
    this.children = new LinkedHashMap<>();
  }

  /**
   * check whether the MNode has a child with the name
   */
  public boolean hasChild(String name) {
    return this.children.containsKey(name) ||
        (aliasChildren != null && aliasChildren.containsKey(name));
  }

  /**
   * node key, name or alias
   */
  public void addChild(String name, MNode child) {
    children.put(name, child);
  }

  /**
   * If delete a leafMNode, lock its parent, if delete an InternalNode, lock itself
   */
  public void deleteChild(String name) throws DeleteFailedException {
    if (children.containsKey(name)) {
      Lock writeLock;
      // if its child node is leaf node, we need to acquire the write lock of the current device node
      if (children.get(name) instanceof MeasurementMNode) {
        writeLock = lock.writeLock();
      } else {
        // otherwise, we only need to acquire the write lock of its child node.
        writeLock = (children.get(name)).lock.writeLock();
      }
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
    return children.containsKey(name) ? children.get(name)
        : (aliasChildren == null ? null : aliasChildren.get(name));
  }

  /**
   * get the count of all leaves whose ancestor is current node
   */
  public int getLeafCount() {
    int leafCount = 0;
    for (MNode child : this.children.values()) {
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
    if (fullPath != null) {
      return fullPath;
    }
    fullPath = concatFullPath();
    return fullPath;
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

  public Map<String, MNode> getChildren() {
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

  public void setAliasChildren(Map<String, MNode> aliasChildren) {
    this.aliasChildren = aliasChildren;
  }

  public void serializeTo(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(MetadataConstant.MNODE_TYPE, outputStream);
    ReadWriteIOUtils.write(name, outputStream);
    serializeChildren(outputStream);
  }

  void serializeChildren(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(children.size(), outputStream);
    for (Entry<String, MNode> entry : children.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serializeTo(outputStream);
    }

    if (aliasChildren == null) {
      ReadWriteIOUtils.write(0, outputStream);
    } else {
      ReadWriteIOUtils.write(aliasChildren.size(), outputStream);
      for (Entry<String, MNode> entry : aliasChildren.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        entry.getValue().serializeTo(outputStream);
      }
    }
  }

  public static MNode deserializeFrom(InputStream inputStream, MNode parent) throws IOException {
    short nodeType = ReadWriteIOUtils.readShort(inputStream);
    MNode node;
    if (nodeType == MetadataConstant.STORAGE_GROUP_MNODE_TYPE) {
      return StorageGroupMNode.deserializeFrom(inputStream, parent);
    } else if (nodeType == MetadataConstant.MEASUREMENT_MNODE_TYPE) {
      return MeasurementMNode.deserializeFrom(inputStream, parent);
    } else {
      node = new MNode(parent, ReadWriteIOUtils.readString(inputStream));
    }

    int childrenSize = ReadWriteIOUtils.readInt(inputStream);
    Map<String, MNode> children = new HashMap<>();
    for (int i = 0; i < childrenSize; i++) {
      children.put(ReadWriteIOUtils.readString(inputStream),
          MNode.deserializeFrom(inputStream, node));
    }
    node.setChildren(children);

    int aliasChildrenSize = ReadWriteIOUtils.readInt(inputStream);
    Map<String, MNode> aliasChildren = new HashMap<>();
    for (int i = 0; i < aliasChildrenSize; i++) {
      children.put(ReadWriteIOUtils.readString(inputStream),
          MNode.deserializeFrom(inputStream, node));
    }
    node.setAliasChildren(aliasChildren);

    return node;
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
