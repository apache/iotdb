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

import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.basic.CachedBasicMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container.CachedMNodeContainer;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class CachedBasicInternalMNode extends CachedBasicMNode {

  /**
   * suppress warnings reason: volatile for double synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  private transient volatile IMNodeContainer<ICachedMNode> children = null;

  /** Constructor of MNode. */
  public CachedBasicInternalMNode(ICachedMNode parent, String name) {
    super(parent, name);
  }

  /** check whether the MNode has a child with the name */
  @Override
  public boolean hasChild(String name) {
    return (children != null && children.containsKey(name));
  }

  /** get the child with the name */
  @Override
  public ICachedMNode getChild(String name) {
    ICachedMNode child = null;
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
  public ICachedMNode addChild(String name, ICachedMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = new CachedMNodeContainer();
        }
      }
    }
    child.setParent(this);
    ICachedMNode existingChild = children.putIfAbsent(name, child);
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
  public ICachedMNode addChild(ICachedMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = new CachedMNodeContainer();
        }
      }
    }

    child.setParent(this);
    children.putIfAbsent(child.getName(), child);
    return child;
  }

  /** delete a child */
  @Override
  public ICachedMNode deleteChild(String name) {
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
  public synchronized void replaceChild(String oldChildName, ICachedMNode newChildNode) {
    if (!oldChildName.equals(newChildNode.getName())) {
      throw new RuntimeException("New child's name must be the same as old child's name!");
    }
    ICachedMNode oldChildNode = this.getChild(oldChildName);
    if (oldChildNode == null) {
      return;
    }

    oldChildNode.moveDataToNewMNode(newChildNode);

    children.replace(newChildNode.getName(), newChildNode);
  }

  @Override
  public void moveDataToNewMNode(ICachedMNode newMNode) {
    super.moveDataToNewMNode(newMNode);

    if (children != null) {
      newMNode.setChildren(children);
      children.forEach((childName, childNode) -> childNode.setParent(newMNode));
    }
  }

  @Override
  public IMNodeContainer<ICachedMNode> getChildren() {
    if (children == null) {
      return CachedMNodeContainer.emptyMNodeContainer();
    }
    return children;
  }

  @Override
  public void setChildren(IMNodeContainer<ICachedMNode> children) {
    this.children = children;
  }

  /**
   * MNodeContainer reference and basic occupation, 8 + 80B.
   *
   * <ol>
   *   Estimated delta size of CachedMNodeContainer compared with other MNodeContainer.
   *   <li>address, 8B
   *   <li>three map reference (1 cache and 2 buffer), 8 * 3 = 24B
   *   <li>estimate occupation of map implementation, minus the basic container occupation, 80 * 3 -
   *       80 = 160B
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + 80 + 192 + super.estimateSize();
  }

  @Override
  public ICachedMNode getAsMNode() {
    return this;
  }
}
