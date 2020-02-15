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

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class LeafMNode extends MNode {

  private static final long serialVersionUID = -1199657856921206435L;

  public LeafMNode(String name, MNode parent) {
    super(name, parent);
  }

  public LeafMNode(String name, MNode parent, TSDataType dataType, TSEncoding encoding,
      CompressionType type) {
    this(name, parent);
    this.schema = new MeasurementSchema(name, dataType, encoding, type);
  }

  @Override
  public boolean isNodeType(MNodeType nodeType) {
    return nodeType.equals(MNodeType.LEAF_MNODE);
  }

  /**
   * check whether the MNode has children
   */
  @Override
  public boolean hasChildren() {
    return false;
  }

  /**
   * check whether the MNode has child with the given key
   *
   * @param key key
   */
  @Override
  public boolean hasChildWithKey(String key) {
    return false;
  }

  @Override
  public void addChild(String key, MNode child) {
  }

  @Override
  public void deleteChild(String key) {
  }

  /**
   * get the child MNode under the given key.
   *
   * @param key key
   */
  @Override
  public MNode getChild(String key) {
    return null;
  }

  /**
   * get the count of all leaves whose ancestor is current node
   */
  @Override
  public int getLeafCount() {
    return 1;
  }
}