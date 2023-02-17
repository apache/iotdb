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

import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.container.MNodeContainers;
import org.apache.iotdb.db.metadata.mnode.visitor.MNodeVisitor;

import static org.apache.iotdb.db.metadata.MetadataConstant.NON_TEMPLATE;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class InternalMNode extends MNode {

  private static final long serialVersionUID = -770028375899514063L;

  /**
   * use in Measurement Node so it's protected suppress warnings reason: volatile for double
   * synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  protected transient volatile IMNodeContainer children = null;

  /**
   * This field is mainly used in cluster schema template features. In InternalMNode of ConfigMTree,
   * this field represents the template set on this node. In EntityMNode of MTree in SchemaRegion,
   * this field represents the template activated on this node. The normal usage value range is [0,
   * Int.MaxValue], since this is implemented as auto inc id. The default value -1 means
   * NON_TEMPLATE. This value will be set negative to implement some pre-delete features.
   */
  protected int schemaTemplateId = NON_TEMPLATE;

  private volatile boolean useTemplate = false;

  /** Constructor of MNode. */
  public InternalMNode(IMNode parent, String name) {
    super(parent, name);
  }

  /** check whether the MNode has a child with the name */
  @Override
  public boolean hasChild(String name) {
    return (children != null && children.containsKey(name));
  }

  /** get the child with the name */
  @Override
  public IMNode getChild(String name) {
    IMNode child = null;
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
  public IMNode addChild(String name, IMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = MNodeContainers.getNewMNodeContainer();
        }
      }
    }
    child.setParent(this);
    IMNode existingChild = children.putIfAbsent(name, child);
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
  public IMNode addChild(IMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = MNodeContainers.getNewMNodeContainer();
        }
      }
    }

    child.setParent(this);
    children.putIfAbsent(child.getName(), child);
    return child;
  }

  /** delete a child */
  @Override
  public IMNode deleteChild(String name) {
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
  public synchronized void replaceChild(String oldChildName, IMNode newChildNode) {
    if (!oldChildName.equals(newChildNode.getName())) {
      throw new RuntimeException("New child's name must be the same as old child's name!");
    }
    IMNode oldChildNode = this.getChild(oldChildName);
    if (oldChildNode == null) {
      return;
    }

    oldChildNode.moveDataToNewMNode(newChildNode);

    children.replace(newChildNode.getName(), newChildNode);
  }

  @Override
  public void moveDataToNewMNode(IMNode newMNode) {
    super.moveDataToNewMNode(newMNode);

    newMNode.setUseTemplate(useTemplate);
    newMNode.setSchemaTemplateId(schemaTemplateId);

    if (children != null) {
      newMNode.setChildren(children);
      children.forEach((childName, childNode) -> childNode.setParent(newMNode));
    }
  }

  @Override
  public IMNodeContainer getChildren() {
    if (children == null) {
      return MNodeContainers.emptyMNodeContainer();
    }
    return children;
  }

  @Override
  public void setChildren(IMNodeContainer children) {
    this.children = children;
  }

  @Override
  public int getSchemaTemplateId() {
    return schemaTemplateId >= -1 ? schemaTemplateId : -schemaTemplateId - 2;
  }

  @Override
  public int getSchemaTemplateIdWithState() {
    return schemaTemplateId;
  }

  @Override
  public void setSchemaTemplateId(int schemaTemplateId) {
    this.schemaTemplateId = schemaTemplateId;
  }

  /**
   * In InternalMNode, schemaTemplateId represents the template set on this node. The pre unset
   * mechanism is implemented by making this value negative. Since value 0 and -1 are all occupied,
   * the available negative value range is [Int.MIN_VALUE, -2]. The value of a pre unset case equals
   * the negative normal value minus 2. For example, if the id of set template is 0, then - 0 - 2 =
   * -2 represents the pre unset operation of this template on this node.
   */
  @Override
  public void preUnsetSchemaTemplate() {
    if (this.schemaTemplateId > -1) {
      this.schemaTemplateId = -schemaTemplateId - 2;
    }
  }

  @Override
  public void rollbackUnsetSchemaTemplate() {
    if (schemaTemplateId < -1) {
      schemaTemplateId = -schemaTemplateId - 2;
    }
  }

  @Override
  public boolean isSchemaTemplatePreUnset() {
    return schemaTemplateId < -1;
  }

  @Override
  public void unsetSchemaTemplate() {
    this.schemaTemplateId = -1;
  }

  @Override
  public boolean isAboveDatabase() {
    return false;
  }

  @Override
  public MNodeType getMNodeType(Boolean isConfig) {
    return isConfig ? MNodeType.SG_INTERNAL : MNodeType.INTERNAL;
  }

  @Override
  public boolean isUseTemplate() {
    return useTemplate;
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {
    this.useTemplate = useTemplate;
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitInternalMNode(this, context);
  }
}
