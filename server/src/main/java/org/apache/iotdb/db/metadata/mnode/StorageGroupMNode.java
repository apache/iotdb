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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.metadata.MetadataConstant;

public class StorageGroupMNode extends MNode {

  private static final long serialVersionUID = 7999036474525817732L;

  /**
   * when the data file in a storage group is older than dataTTL, it is considered invalid and will
   * be eventually deleted.
   */
  private long dataTTL;

  public StorageGroupMNode(MNode parent, String name, long dataTTL) {
    super(parent, name);
    this.dataTTL = dataTTL;
    this.fullPath = getFullPath();
  }

  public long getDataTTL() {
    return dataTTL;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }

  @Override
  public void serializeTo(BufferedWriter bw) throws IOException {
    String s = String.valueOf(MetadataConstant.STORAGE_GROUP_MNODE_TYPE);
    s += "," + name + ",";
    s += dataTTL + ",";
    s += children.size() + ",";
    s += aliasChildren == null ? 0 : aliasChildren.size();
    bw.write(s);
    bw.newLine();
    serializeChildren(bw);
  }

  public static StorageGroupMNode deserializeFrom(BufferedReader br, String[] nodeInfo,
      MNode parent) throws IOException {
    StorageGroupMNode node = new StorageGroupMNode(parent, nodeInfo[1], Long.valueOf(nodeInfo[2]));

    Map<String, MNode> children = new HashMap<>();
    for (int i = 0; i < Integer.valueOf(nodeInfo[3]); i++) {
      MNode child = MNode.deserializeFrom(br, node);
      children.put(child.getName(), child);
    }
    node.setChildren(children);

    Map<String, MNode> aliasChildren = new HashMap<>();
    for (int i = 0; i < Integer.valueOf(nodeInfo[4]); i++) {
      MNode child = MNode.deserializeFrom(br, node);
      children.put(child.getName(), child);
    }
    node.setAliasChildren(aliasChildren);

    return node;
  }
}