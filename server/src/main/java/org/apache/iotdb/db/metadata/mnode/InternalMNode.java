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

import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

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
  protected transient volatile Map<String, IMNode> children = null;

  // schema template
  protected Template schemaTemplate = null;

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
   */
  @Override
  public void addChild(String name, IMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = new ConcurrentHashMap<>();
        }
      }
    }
    child.setParent(this);
    children.putIfAbsent(name, child);
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
  public IMNode addChild(IMNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = new ConcurrentHashMap<>();
        }
      }
    }

    child.setParent(this);
    children.putIfAbsent(child.getName(), child);
    return child;
  }

  /** delete a child */
  @Override
  public void deleteChild(String name) {
    if (children != null) {
      children.remove(name);
    }
  }

  /**
   * replace a child of this mnode
   *
   * @param oldChildName measurement name
   * @param newChildNode new child node
   */
  @Override
  public void replaceChild(String oldChildName, IMNode newChildNode) {
    IMNode oldChildNode = this.getChild(oldChildName);
    if (oldChildNode == null) {
      return;
    }

    // newChildNode builds parent-child relationship
    Map<String, IMNode> grandChildren = oldChildNode.getChildren();
    if (!grandChildren.isEmpty()) {
      newChildNode.setChildren(grandChildren);
      grandChildren.forEach(
          (grandChildName, grandChildNode) -> grandChildNode.setParent(newChildNode));
    }

    if (newChildNode.isEntity() && oldChildNode.isEntity()) {
      Map<String, IMeasurementMNode> grandAliasChildren =
          oldChildNode.getAsEntityMNode().getAliasChildren();
      if (!grandAliasChildren.isEmpty()) {
        newChildNode.getAsEntityMNode().setAliasChildren(grandAliasChildren);
        grandAliasChildren.forEach(
            (grandAliasChildName, grandAliasChild) -> grandAliasChild.setParent(newChildNode));
      }
      newChildNode.getAsEntityMNode().setUseTemplate(oldChildNode.isUseTemplate());
    }

    newChildNode.setSchemaTemplate(oldChildNode.getSchemaTemplate());

    newChildNode.setParent(this);

    this.deleteChild(oldChildName);
    this.addChild(newChildNode.getName(), newChildNode);
  }

  @Override
  public Map<String, IMNode> getChildren() {
    if (children == null) {
      return Collections.emptyMap();
    }
    return children;
  }

  @Override
  public void setChildren(Map<String, IMNode> children) {
    this.children = children;
  }

  /**
   * get upper template of this node, remember we get nearest template alone this node to root
   *
   * @return upper template
   */
  @Override
  public Template getUpperTemplate() {
    IMNode cur = this;
    while (cur != null) {
      if (cur.getSchemaTemplate() != null) {
        return cur.getSchemaTemplate();
      }
      cur = cur.getParent();
    }

    return null;
  }

  @Override
  public Template getSchemaTemplate() {
    return schemaTemplate;
  }

  @Override
  public void setSchemaTemplate(Template schemaTemplate) {
    this.schemaTemplate = schemaTemplate;
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {
    serializeChildren(logWriter);

    logWriter.serializeMNode(this);
  }

  void serializeChildren(MLogWriter logWriter) throws IOException {
    if (children == null) {
      return;
    }
    for (Entry<String, IMNode> entry : children.entrySet()) {
      entry.getValue().serializeTo(logWriter);
    }
  }

  public static InternalMNode deserializeFrom(MNodePlan plan) {
    return new InternalMNode(null, plan.getName());
  }
}
