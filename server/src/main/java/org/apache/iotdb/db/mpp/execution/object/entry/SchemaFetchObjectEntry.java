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

package org.apache.iotdb.db.mpp.execution.object.entry;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.execution.object.ObjectEntry;
import org.apache.iotdb.db.mpp.execution.object.ObjectType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class SchemaFetchObjectEntry extends ObjectEntry {

  private List<String> storageGroupList;

  private ClusterSchemaTree schemaTree;

  private transient boolean isReadingStorageGroupInfo;

  public SchemaFetchObjectEntry() {
    super();
  }

  public SchemaFetchObjectEntry(List<String> storageGroupList) {
    super();
    this.storageGroupList = storageGroupList;
    isReadingStorageGroupInfo = true;
  }

  public SchemaFetchObjectEntry(ClusterSchemaTree schemaTree) {
    super();
    this.schemaTree = schemaTree;
    isReadingStorageGroupInfo = false;
  }

  @Override
  public ObjectType getType() {
    return ObjectType.SCHEMA_FETCH;
  }

  public boolean isReadingStorageGroupInfo() {
    return isReadingStorageGroupInfo;
  }

  public List<String> getStorageGroupList() {
    return storageGroupList;
  }

  public ClusterSchemaTree getSchemaTree() {
    return schemaTree;
  }

  @Override
  protected void serializeObjectData(DataOutputStream dataOutputStream) throws IOException {
    if (isReadingStorageGroupInfo) {
      // to indicate this binary data is storage group info
      ReadWriteIOUtils.write((byte) 0, dataOutputStream);

      ReadWriteIOUtils.write(storageGroupList.size(), dataOutputStream);
      for (String storageGroup : storageGroupList) {
        ReadWriteIOUtils.write(storageGroup, dataOutputStream);
      }
    } else {
      // to indicate this binary data is storage group info
      ReadWriteIOUtils.write((byte) 1, dataOutputStream);

      schemaTree.serialize(dataOutputStream);
    }
  }

  @Override
  protected void deserializeObjectData(DataInputStream dataInputStream) throws IOException {
    byte type = ReadWriteIOUtils.readByte(dataInputStream);
    if (type == 0) {
      int size = ReadWriteIOUtils.readInt(dataInputStream);
      for (int i = 0; i < size; i++) {
        storageGroupList.add(ReadWriteIOUtils.readString(dataInputStream));
      }
    } else if (type == 1) {
      schemaTree = ClusterSchemaTree.deserialize(dataInputStream);
    } else {
      throw new RuntimeException(
          new MetadataException("Failed to fetch schema because of unrecognized data"));
    }
  }
}
