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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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
  public void serializeTo(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(MetadataConstant.STORAGE_GROUP_MNODE_TYPE, outputStream);
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(dataTTL, outputStream);

    serializeChildren(outputStream);
  }

  public static StorageGroupMNode deserializeFrom(InputStream inputStream, MNode parent)
      throws IOException {
    String name = ReadWriteIOUtils.readString(inputStream);
    StorageGroupMNode node = new StorageGroupMNode(parent, name,
        ReadWriteIOUtils.readLong(inputStream));

    int childrenSize = ReadWriteIOUtils.readInt(inputStream);
    Map<String, MNode> children = new HashMap<>();
    for (int i = 0; i < childrenSize; i++) {
      children.put(ReadWriteIOUtils.readString(inputStream),
          MNode.deserializeFrom(inputStream, node));
    }
    node.setChildren(children);

    int aliasChildrenSize = ReadWriteIOUtils.readInt(inputStream);
    Map<String, MNode> aliasChildren = new HashMap<>();
    for (int i = 0; i < aliasChildrenSize; i++) {
      children.put(ReadWriteIOUtils.readString(inputStream),
          MNode.deserializeFrom(inputStream, node));
    }
    node.setAliasChildren(aliasChildren);

    return node;
  }
}