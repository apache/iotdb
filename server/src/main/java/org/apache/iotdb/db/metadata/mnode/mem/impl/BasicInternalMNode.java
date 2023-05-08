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
package org.apache.iotdb.db.metadata.mnode.mem.impl;

import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.mem.IMemMNode;
import org.apache.iotdb.db.metadata.mnode.mem.basic.BasicMNode;
import org.apache.iotdb.db.metadata.mnode.mem.container.MemMNodeContainer;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class BasicInternalMNode extends BasicMNode {

  /**
   * suppress warnings reason: volatile for double synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  private transient volatile IMNodeContainer<IMemMNode> children = null;

  /** Constructor of MNode. */
  public BasicInternalMNode(IMemMNode parent, String name) {
    super(parent, name);
  }

  /** check whether the MNode has a child with the name */
  @Override
  public boolean hasChild(String name) {
    return (children != null && children.containsKey(name));
  }

  /** get the child with the name */
  @Override
  public IMemMNode getChild(String name) {
    IMemMNode child = null;
    if (children != null) {
      child = children.get(name);
    }
    return child;
  }

  /**
   * add a child to current mnode
   *
   * @param name child's name
   * @param child child's node
   * @return the child of this node after addChild
   */
  @Override
  public IMemMNode addChild(String name, IMemMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = new MemMNodeContainer();
        }
      }
    }
    child.setParent(this);
    IMemMNode existingChild = children.putIfAbsent(name, child);
    return existingChild == null ? child : existingChild;
  }

  /**
   * Add a child to the current mnode.
   *
   * <p>This method will not take the child's name as one of the inputs and will also make this
   * Mnode be child node's parent. All is to reduce the probability of mistaken by users and be more
   * convenient for users to use. And the return of this method is used to conveniently construct a
   * chain of time series for users.
   *
   * @param child child's node
   * @return return the MNode already added
   */
  @Override
  public IMemMNode addChild(IMemMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = new MemMNodeContainer();
        }
      }
    }

    child.setParent(this);
    children.putIfAbsent(child.getName(), child);
    return child;
  }

  /** delete a child */
  @Override
  public IMemMNode deleteChild(String name) {
    if (children != null) {
      return children.remove(name);
    }
    return null;
  }

  /**
   * Replace a child of this mnode. New child's name must be the same as old child's name.
   *
   * @param oldChildName measurement name
   * @param newChildNode new child node
   */
  @Override
  public synchronized void replaceChild(String oldChildName, IMemMNode newChildNode) {
    if (!oldChildName.equals(newChildNode.getName())) {
      throw new RuntimeException("New child's name must be the same as old child's name!");
    }
    IMemMNode oldChildNode = this.getChild(oldChildName);
    if (oldChildNode == null) {
      return;
    }

    oldChildNode.moveDataToNewMNode(newChildNode);

    children.replace(newChildNode.getName(), newChildNode);
  }

  @Override
  public void moveDataToNewMNode(IMemMNode newMNode) {
    super.moveDataToNewMNode(newMNode);

    if (children != null) {
      newMNode.setChildren(children);
      children.forEach((childName, childNode) -> childNode.setParent(newMNode));
    }
  }

  @Override
  public IMNodeContainer<IMemMNode> getChildren() {
    if (children == null) {
      return MemMNodeContainer.emptyMNodeContainer();
    }
    return children;
  }

  @Override
  public void setChildren(IMNodeContainer<IMemMNode> children) {
    this.children = children;
  }

  /** MNodeContainer reference and basic occupation, 8 + 80B */
  @Override
  public int estimateSize() {
    return 8 + 80 + super.estimateSize();
  }

  @Override
  public IMemMNode getAsMNode() {
    return this;
  }
}
