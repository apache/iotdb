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

package org.apache.iotdb.cluster.log.snapshot;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.utils.SerializeUtils;

/**
 * MetaSimpleSnapshot also records all storage groups.
 */
public class MetaSimpleSnapshot extends SimpleSnapshot {

  private List<String> storageGroups;
  private Map<String, Long> storageGroupTTL;
  private Map<String, Boolean> userWaterMarkStatus;

  public MetaSimpleSnapshot() {
    storageGroups = Collections.emptyList();
    storageGroupTTL = Collections.emptyMap();
    userWaterMarkStatus = Collections.emptyMap();
  }

  public MetaSimpleSnapshot(
      List<Log> snapshot,
      List<String> storageGroups,
      Map<String, Long> storageGroupTTL,
      Map<String, Boolean> userWaterMarkStatus) {
    super(snapshot);
    this.storageGroups = storageGroups;
    this.storageGroupTTL = storageGroupTTL;
    this.userWaterMarkStatus = userWaterMarkStatus;

  }

  public List<String> getStorageGroups() {
    return storageGroups;
  }

  public Map<String, Long> getStorageGroupTTL() {
    return storageGroupTTL;
  }

  public Map<String, Boolean> getUserWaterMarkStatus() {
    return userWaterMarkStatus;
  }

  @Override
  void subSerialize(DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(storageGroups.size());
      for (String storageGroup : storageGroups) {
        SerializeUtils.serialize(storageGroup, dataOutputStream);
      }

      dataOutputStream.writeInt(storageGroupTTL.size());
      for (Map.Entry<String, Long> entry : storageGroupTTL.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        dataOutputStream.writeLong(entry.getValue());
      }

      dataOutputStream.writeInt(userWaterMarkStatus.size());
      for (Map.Entry<String, Boolean> entry : userWaterMarkStatus.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        dataOutputStream.writeBoolean(entry.getValue());
      }
    } catch (IOException e) {
      // unreachable
    }
  }

  @Override
  void subDeserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    storageGroups = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      storageGroups.add(SerializeUtils.deserializeString(buffer));
    }

    int sgtSize = buffer.getInt();
    storageGroupTTL = new HashMap<>();
    for (int i = 0; i < sgtSize; i++) {
      storageGroupTTL.put(SerializeUtils.deserializeString(buffer), buffer.getLong());
    }

    int uwmSize = buffer.getInt();
    userWaterMarkStatus = new HashMap<>();
    for (int i = 0; i < uwmSize; i++) {
      userWaterMarkStatus.put(SerializeUtils.deserializeString(buffer), buffer.get() == 1);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MetaSimpleSnapshot snapshot = (MetaSimpleSnapshot) o;
    return Objects.equals(storageGroups, snapshot.storageGroups) &&
        Objects.equals(storageGroupTTL, snapshot.getStorageGroupTTL()) &&
        Objects.equals(userWaterMarkStatus, snapshot.getUserWaterMarkStatus());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), storageGroups, storageGroupTTL, userWaterMarkStatus);
  }
}
