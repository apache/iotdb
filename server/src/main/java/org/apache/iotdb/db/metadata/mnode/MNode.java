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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.rescon.CachedStringPool;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class MNode implements Serializable {

  private static final long serialVersionUID = -770028375899514063L;
  private static Map<String, String> cachedPathPool =
      CachedStringPool.getInstance().getCachedPool();

  /** Name of the MNode */
  protected String name;

  protected MNode parent;

  /** from root to this node, only be set when used once for InternalMNode */
  protected String fullPath;

  /**
   * use in Measurement Node so it's protected suppress warnings reason: volatile for double
   * synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  protected transient volatile Map<String, MNode> children = null;

  /**
   * suppress warnings reason: volatile for double synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  private transient volatile Map<String, MNode> aliasChildren = null;

  // device template
  protected Template deviceTemplate = null;

  private volatile boolean useTemplate = false;

  /** Constructor of MNode. */
  public MNode(MNode parent, String name) {
    this.parent = parent;
    this.name = name;
  }

  /** check whether the MNode has a child with the name */
  public boolean hasChild(String name) {
    return (children != null && children.containsKey(name))
        || (aliasChildren != null && aliasChildren.containsKey(name));
  }

  /**
   * add a child to current mnode
   *
   * @param name child's name
   * @param child child's node
   */
  public void addChild(String name, MNode child) {
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

    child.parent = this;
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
  MNode addChild(MNode child) {
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

    child.parent = this;
    children.putIfAbsent(child.getName(), child);
    return child;
  }

  /** delete a child */
  public void deleteChild(String name) {
    if (children != null) {
      children.remove(name);
    }
  }

  /** delete the alias of a child */
  public void deleteAliasChild(String alias) {
    if (aliasChildren != null) {
      aliasChildren.remove(alias);
    }
  }

  public Template getDeviceTemplate() {
    return deviceTemplate;
  }

  public void setDeviceTemplate(Template deviceTemplate) {
    this.deviceTemplate = deviceTemplate;
  }

  /** get the child with the name */
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

  /** get the count of all MeasurementMNode whose ancestor is current node */
  public int getMeasurementMNodeCount() {
    if (children == null) {
      return 1;
    }
    int measurementMNodeCount = 0;
    if (this instanceof MeasurementMNode) {
      measurementMNodeCount += 1; // current node itself may be MeasurementMNode
    }
    for (MNode child : children.values()) {
      measurementMNodeCount += child.getMeasurementMNodeCount();
    }
    return measurementMNodeCount;
  }

  /** add an alias */
  public boolean addAlias(String alias, MNode child) {
    if (aliasChildren == null) {
      // double check, alias children volatile
      synchronized (this) {
        if (aliasChildren == null) {
          aliasChildren = new ConcurrentHashMap<>();
        }
      }
    }

    return aliasChildren.computeIfAbsent(alias, aliasName -> child) == child;
  }

  /** get full path */
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
      return Collections.emptyMap();
    }
    return children;
  }

  public Map<String, MNode> getAliasChildren() {
    if (aliasChildren == null) {
      return Collections.emptyMap();
    }
    return aliasChildren;
  }

  public void setChildren(Map<String, MNode> children) {
    this.children = children;
  }

  private void setAliasChildren(Map<String, MNode> aliasChildren) {
    this.aliasChildren = aliasChildren;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void serializeTo(MLogWriter logWriter) throws IOException {
    serializeChildren(logWriter);

    logWriter.serializeMNode(this);
  }

  void serializeChildren(MLogWriter logWriter) throws IOException {
    if (children == null) {
      return;
    }
    for (Entry<String, MNode> entry : children.entrySet()) {
      entry.getValue().serializeTo(logWriter);
    }
  }

  public void replaceChild(String measurement, MNode newChildNode) {
    MNode oldChildNode = this.getChild(measurement);
    if (oldChildNode == null) {
      return;
    }

    // newChildNode builds parent-child relationship
    Map<String, MNode> grandChildren = oldChildNode.getChildren();
    newChildNode.setChildren(grandChildren);
    grandChildren.forEach(
        (grandChildName, grandChildNode) -> grandChildNode.setParent(newChildNode));

    Map<String, MNode> grandAliasChildren = oldChildNode.getAliasChildren();
    newChildNode.setAliasChildren(grandAliasChildren);
    grandAliasChildren.forEach(
        (grandAliasChildName, grandAliasChild) -> grandAliasChild.setParent(newChildNode));

    newChildNode.setParent(this);

    this.deleteChild(measurement);
    this.addChild(newChildNode.getName(), newChildNode);
  }

  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }

  /**
   * get upper template of this node, remember we get nearest template alone this node to root
   *
   * @return upper template
   */
  public Template getUpperTemplate() {
    MNode cur = this;
    while (cur != null) {
      if (cur.getDeviceTemplate() != null) {
        return cur.deviceTemplate;
      }
      cur = cur.parent;
    }

    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MNode mNode = (MNode) o;
    if (fullPath == null) {
      return Objects.equals(getFullPath(), mNode.getFullPath());
    } else {
      return Objects.equals(fullPath, mNode.getFullPath());
    }
  }

  @Override
  public int hashCode() {
    if (fullPath == null) {
      return Objects.hash(getFullPath());
    } else {
      return Objects.hash(fullPath);
    }
  }

  public boolean isUseTemplate() {
    return useTemplate;
  }

  public void setUseTemplate(boolean useTemplate) {
    this.useTemplate = useTemplate;
  }
}
