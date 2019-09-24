/**
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
package org.apache.iotdb.db.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * This class is the implementation of Metadata Node where "MNode" is the shorthand of "Metadata
 * Node". One MNode instance represents one node in the Metadata Tree
 */
public class MNode implements Serializable {

  private static final long serialVersionUID = -770028375899514063L;

  // The name of the MNode
  private String name;
  // Whether current node is a leaf in the Metadata Tree
  private boolean isLeaf;
  // Whether current node is Storage group in the Metadata Tree
  private boolean isStorageGroup;
  // Map for the schema in this storage group
  private Map<String, MeasurementSchema> schemaMap;
  private Map<String, Integer> numSchemaMap;
  // Corresponding data file name for current node
  private String dataFileName;
  // Column's Schema for one timeseries represented by current node if current
  // node is one leaf
  private MeasurementSchema schema;
  private MNode parent;
  private Map<String, MNode> children;

  /**
   * Constructor of MNode.
   */
  public MNode(String name, MNode parent, boolean isLeaf) {
    this.setName(name);
    this.parent = parent;
    this.isLeaf = isLeaf;
    this.isStorageGroup = false;
    if (!isLeaf) {
      children = new LinkedHashMap<>();
    }
  }

  public MNode(String name, MNode parent, TSDataType dataType, TSEncoding encoding,
      CompressionType type) {
    this(name, parent, true);
    this.schema = new MeasurementSchema(name, dataType, encoding, type);
  }

  public boolean isStorageGroup() {
    return isStorageGroup;
  }

  /**
   * function for setting storage group.
   */
  public void setStorageGroup(boolean b) {
    this.isStorageGroup = b;
    if (b) {
      schemaMap = new HashMap<>();
      numSchemaMap = new HashMap<>();
    } else {
      numSchemaMap = null;
      schemaMap = null;
    }
  }

  public Map<String, MeasurementSchema> getSchemaMap() {
    return schemaMap;
  }

  public Map<String, Integer> getNumSchemaMap() {
    return numSchemaMap;
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void setLeaf(boolean isLeaf) {
    this.isLeaf = isLeaf;
  }

  /**
   * function for checking whether the mnode has child mnode.
   */
  public boolean hasChildren() {
    if (!isLeaf) {
      return true;
    }
    return false;
  }

  /**
   * function for checking whether mnode's children contain the given key.
   */
  public boolean hasChild(String key) {
    if (!isLeaf) {
      return this.children.containsKey(key);
    }
    return false;
  }

  /**
   * function for adding the given key to the given child mnode.
   */
  public void addChild(String key, MNode child) {
    if (!isLeaf) {
      this.children.put(key, child);
    }
  }

  public void deleteChild(String key) {
    children.remove(key);
  }

  /**
   * function for getting the child mnode under the given key.
   */
  public MNode getChild(String key) {
    if (!isLeaf) {
      return children.get(key);
    }
    return null;
  }

  /**
   * function for getting the count of all leaves whose ancestor is current node.
   */
  public int getLeafCount() {
    if (isLeaf) {
      return 1;
    } else {
      int leafCount = 0;
      for (MNode child : this.children.values()) {
        leafCount += child.getLeafCount();
      }
      return leafCount;
    }
  }

  public String getDataFileName() {
    return dataFileName;
  }

  public void setDataFileName(String dataFileName) {
    this.dataFileName = dataFileName;
  }

  @Override
  public String toString() {
    return this.getName();
  }

  public MeasurementSchema getSchema() {
    return schema;
  }

  public void setSchema(MeasurementSchema schema) {
    this.schema = schema;
  }

  public MNode getParent() {
    return parent;
  }

  public void setParent(MNode parent) {
    this.parent = parent;
  }

  public Map<String, MNode> getChildren() {
    return children;
  }

  public void setChildren(Map<String, MNode> children) {
    this.children = children;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

}
