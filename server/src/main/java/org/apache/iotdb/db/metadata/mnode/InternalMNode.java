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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class InternalMNode extends MNode {

  private static final long serialVersionUID = 7999036474525817732L;

  public InternalMNode(String name, MNode parent) {
    super(name, parent);
    this.children = new LinkedHashMap<>();
  }

  /**
   * check whether the MNode has children
   */
  @Override
  public boolean hasChildren() {
    return true;
  }

  /**
   * check whether the MNode has child with the given key
   *
   * @param childName key
   */
  @Override
  public boolean hasChild(String childName) {
    return this.children.containsKey(childName);
  }

  /**
   * add given child MNode
   *
   * @param child child MNode
   */
  @Override
  public void addChild(MNode child) {
    this.children.put(child.getName(), child);
  }

  /**
   * delete key from given child MNode
   *
   * @param key deleted child name
   */
  @Override
  public void deleteChild(String key) {
    children.remove(key);
  }

  /**
   * get the child MNode under the given key.
   *
   * @param key the queried child name
   */
  @Override
  public MNode getChild(String key) {
    return children.get(key);
  }

  /**
   * get the count of all leaves whose ancestor is current node
   */
  @Override
  public int getLeafCount() {
    int leafCount = 0;
    for (MNode child : this.children.values()) {
      leafCount += child.getLeafCount();
    }
    return leafCount;
  }

  @Override
  public MeasurementSchema getSchema() {
    return null;
  }

  @Override
  public Map<String, MNode> getChildren() {
    return children;
  }
}