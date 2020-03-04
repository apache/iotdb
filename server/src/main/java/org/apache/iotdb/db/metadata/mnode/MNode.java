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
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConstant;
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

  private MNode parent;

  /**
   * from root to this node, only be set when used once
   */
  protected String fullPath;

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
  public abstract boolean hasChild(String name);

  /**
   * add the given child
   */
  public abstract void addChild(MNode child);

  /**
   * delete a child
   */
  public abstract void deleteChild(String name);

  /**
   * get the child with the name
   */
  public abstract MNode getChild(String name);

  /**
   * get the count of all leaves whose ancestor is current node
   */
  public abstract int getLeafCount();

  /**
   * get full path
   */
  public String getFullPath() {
    if (fullPath != null) {
      return fullPath;
    }
    StringBuilder builder = new StringBuilder(name);
    MNode curr = this;
    while (curr.getParent() != null) {
      curr = curr.getParent();
      builder.insert(0, IoTDBConstant.PATH_SEPARATOR).insert(0, curr.name);
    }
    fullPath = builder.toString();
    return fullPath;
  }

  @Override
  public String toString() {
    return this.getName();
  }

  public abstract MeasurementSchema getSchema();

  public MNode getParent() {
    return parent;
  }

  public abstract Map<String, MNode> getChildren();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
