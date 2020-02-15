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

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public abstract class MNode implements Serializable {

  private static final long serialVersionUID = -770028375899514063L;

  /**
   * Name of the MNode
   */
  private String name;

  /**
   * Map for the schema in this storage group
   */
  private Map<String, MeasurementSchema> schemaMap;

  /**
   * Corresponding storage group name for current node
   */
  private String storageGroupName;

  private MNode parent;

  private String fullPath;

  /**
   * Column's Schema for one timeseries represented by current node if current node is one leaf
   */
  MeasurementSchema schema;

  Map<String, MNode> children;

  MNodeType nodeType;

  /**
   * when the data in a storage group is older than dataTTL, it is considered invalid and will be
   * eventually removed. only set at storage group level.
   */
  private long dataTTL = Long.MAX_VALUE;

  /**
   * Constructor of MNode.
   */
  public MNode(String name, MNode parent) {
    this.setName(name);
    this.parent = parent;
  }

  abstract public boolean isNodeType(MNodeType nodeType);

  /**
   * setting storage group.
   */
  public void setStorageGroup(boolean isStorageGroup) {
    this.nodeType = MNodeType.STORAGE_GROUP_MNODE;
    if (isStorageGroup) {
      schemaMap = new HashMap<>();
    } else {
      schemaMap = null;
    }
  }

  public Map<String, MeasurementSchema> getSchemaMap() {
    return schemaMap;
  }

  /**
   * check whether the MNode has children
   */
  abstract public boolean hasChildren();

  /**
   * check whether the MNode has child with the given key
   *
   * @param key key
   */
  abstract public boolean hasChildWithKey(String key);

  /**
   * add the given key to given child MNode
   *
   * @param key key
   * @param child child MNode
   */
  abstract public void addChild(String key, MNode child);

  /**
   * delete key from given child MNode
   *
   * @param key key
   */
  abstract public void deleteChild(String key);

  /**
   * get the child MNode under the given key.
   *
   * @param key key
   */
  abstract public MNode getChild(String key);

  /**
   * get the count of all leaves whose ancestor is current node
   */
  abstract public int getLeafCount();

  /**
   * get full path
   */
  public String getFullPath() {
    if (fullPath != null) {
      return fullPath;
    }
    StringBuilder builder = new StringBuilder(name);
    MNode curr = this;
    while (curr.parent != null) {
      curr = curr.parent;
      builder.insert(0, IoTDBConstant.PATH_SEPARATOR).insert(0, curr.name);
    }
    return fullPath = builder.toString();
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public void setStorageGroupName(String storageGroupName) {
    this.storageGroupName = storageGroupName;
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

  public Map<String, MNode> getChildren() {
    return children;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getDataTTL() {
    return dataTTL;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }
}