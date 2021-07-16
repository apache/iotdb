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
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.rescon.CachedStringPool;

import java.io.IOException;
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
public class MNode implements IMNode {

  private static final long serialVersionUID = -770028375899514063L;
  private static Map<String, String> cachedPathPool =
      CachedStringPool.getInstance().getCachedPool();

  /** Name of the MNode */
  protected String name;

  protected IMNode parent;

  /** from root to this node, only be set when used once for InternalMNode */
  protected String fullPath;

  /**
   * use in Measurement Node so it's protected suppress warnings reason: volatile for double
   * synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  protected transient volatile Map<String, IMNode> children = null;

  /**
   * suppress warnings reason: volatile for double synchronized check
   *
   * <p>This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  private transient volatile Map<String, IMNode> aliasChildren = null;

  // device template
  protected Template deviceTemplate = null;

  private volatile boolean useTemplate = false;

  /** Constructor of MNode. */
  public MNode(IMNode parent, String name) {
    this.parent = parent;
    this.name = name;
  }

  /** check whether the MNode has a child with the name */
  @Override
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

  /** delete the alias of a child */
  @Override
  public void deleteAliasChild(String alias) {
    if (aliasChildren != null) {
      aliasChildren.remove(alias);
    }
  }

  @Override
  public Template getDeviceTemplate() {
    return deviceTemplate;
  }

  @Override
  public void setDeviceTemplate(Template deviceTemplate) {
    this.deviceTemplate = deviceTemplate;
  }

  /** get the child with the name */
  @Override
  public IMNode getChild(String name) {
    IMNode child = null;
    if (children != null) {
      child = children.get(name);
    }
    if (child != null) {
      return child;
    }
    return aliasChildren == null ? null : aliasChildren.get(name);
  }

  @Override
  public IMNode getChildOfAlignedTimeseries(String name) throws MetadataException {
    IMNode node = null;
    // for aligned timeseries
    List<String> measurementList = MetaUtils.getMeasurementsInPartialPath(new PartialPath(name));
    for (String measurement : measurementList) {
      IMNode nodeOfMeasurement = getChild(measurement);
      if (node == null) {
        node = nodeOfMeasurement;
      } else {
        if (node != nodeOfMeasurement) {
          throw new AlignedTimeseriesException(
              "Cannot get node of children in different aligned timeseries", name);
        }
      }
    }
    return node;
  }

  /** get the count of all MeasurementMNode whose ancestor is current node */
  @Override
  public int getMeasurementMNodeCount() {
    if (children == null) {
      return 1;
    }
    int measurementMNodeCount = 0;
    if (this instanceof MeasurementMNode) {
      measurementMNodeCount += 1; // current node itself may be MeasurementMNode
    }
    for (IMNode child : children.values()) {
      measurementMNodeCount += child.getMeasurementMNodeCount();
    }
    return measurementMNodeCount;
  }

  /** add an alias */
  @Override
  public boolean addAlias(String alias, IMNode child) {
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
  @Override
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

  /**
   * get partial path of this node
   *
   * @return partial path
   */
  @Override
  public PartialPath getPartialPath() {
    List<String> detachedPath = new ArrayList<>();
    IMNode temp = this;
    detachedPath.add(temp.getName());
    while (temp.getParent() != null) {
      temp = temp.getParent();
      detachedPath.add(0, temp.getName());
    }
    return new PartialPath(detachedPath.toArray(new String[0]));
  }

  String concatFullPath() {
    StringBuilder builder = new StringBuilder(name);
    IMNode curr = this;
    while (curr.getParent() != null) {
      curr = curr.getParent();
      builder.insert(0, IoTDBConstant.PATH_SEPARATOR).insert(0, curr.getName());
    }
    return builder.toString();
  }

  @Override
  public String toString() {
    return this.getName();
  }

  @Override
  public IMNode getParent() {
    return parent;
  }

  @Override
  public void setParent(IMNode parent) {
    this.parent = parent;
  }

  @Override
  public Map<String, IMNode> getChildren() {
    if (children == null) {
      return Collections.emptyMap();
    }
    return children;
  }

  @Override
  public Map<String, IMNode> getAliasChildren() {
    if (aliasChildren == null) {
      return Collections.emptyMap();
    }
    return aliasChildren;
  }

  @Override
  public void setChildren(Map<String, IMNode> children) {
    this.children = children;
  }

  public void setAliasChildren(Map<String, IMNode> aliasChildren) {
    this.aliasChildren = aliasChildren;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
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

  /**
   * replace a child of this mnode
   *
   * @param measurement measurement name
   * @param newChildNode new child node
   */
  @Override
  public void replaceChild(String measurement, IMNode newChildNode) {
    IMNode oldChildNode = this.getChild(measurement);
    if (oldChildNode == null) {
      return;
    }

    // newChildNode builds parent-child relationship
    Map<String, IMNode> grandChildren = oldChildNode.getChildren();
    newChildNode.setChildren(grandChildren);
    grandChildren.forEach(
        (grandChildName, grandChildNode) -> grandChildNode.setParent(newChildNode));

    Map<String, IMNode> grandAliasChildren = oldChildNode.getAliasChildren();
    newChildNode.setAliasChildren(grandAliasChildren);
    grandAliasChildren.forEach(
        (grandAliasChildName, grandAliasChild) -> grandAliasChild.setParent(newChildNode));

    newChildNode.setParent(this);

    this.deleteChild(measurement);
    this.addChild(newChildNode.getName(), newChildNode);
  }

  @Override
  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
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
      if (cur.getDeviceTemplate() != null) {
        return cur.getDeviceTemplate();
      }
      cur = cur.getParent();
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
      return Objects.equals(fullPath, mNode.fullPath);
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

  @Override
  public boolean isUseTemplate() {
    return useTemplate;
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {
    this.useTemplate = useTemplate;
  }
}
