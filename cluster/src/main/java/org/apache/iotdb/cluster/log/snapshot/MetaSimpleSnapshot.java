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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.utils.SerializeUtils;

/**
 * MetaSimpleSnapshot also records all storage groups.
 */
public class MetaSimpleSnapshot extends SimpleSnapshot {

  private Map<String, Long> storageGroupTTLMap;
  private Map<String, User> userMap;
  private Map<String, Role> roleMap;
  private ByteBuffer partitionTableBuffer;

  public MetaSimpleSnapshot() {
    storageGroupTTLMap = Collections.emptyMap();
    userMap = Collections.emptyMap();
    roleMap = Collections.emptyMap();
    partitionTableBuffer = null;
  }

  public MetaSimpleSnapshot(
      Map<String, Long> storageGroupTTLMap,
      Map<String, User> userMap,
      Map<String, Role> roleMap,
      ByteBuffer partitionTableBuffer) {
    this.storageGroupTTLMap = storageGroupTTLMap;
    this.userMap = userMap;
    this.roleMap = roleMap;
    this.partitionTableBuffer = partitionTableBuffer;
  }

  public Map<String, Long> getStorageGroupTTLMap() {
    return storageGroupTTLMap;
  }

  public Map<String, User> getUserMap() {
    return userMap;
  }

  public Map<String, Role> getRoleMap() {
    return roleMap;
  }

  public ByteBuffer getPartitionTableBuffer() {
    return partitionTableBuffer;
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    try {
      dataOutputStream.writeInt(storageGroupTTLMap.size());
      for (Map.Entry<String, Long> entry : storageGroupTTLMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        dataOutputStream.writeLong(entry.getValue());
      }

      dataOutputStream.writeInt(userMap.size());
      for (Map.Entry<String, User> entry : userMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        dataOutputStream.write(entry.getValue().serialize().array());
      }

      dataOutputStream.writeInt(roleMap.size());
      for (Map.Entry<String, Role> entry : roleMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        dataOutputStream.write(entry.getValue().serialize().array());
      }

      dataOutputStream.write(partitionTableBuffer.array());
    } catch (IOException e) {
      // unreachable
    }
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    int storageGroupTTLMapSize = buffer.getInt();
    storageGroupTTLMap = new HashMap<>(storageGroupTTLMapSize);
    for (int i = 0; i < storageGroupTTLMapSize; i++) {
      storageGroupTTLMap.put(SerializeUtils.deserializeString(buffer), buffer.getLong());
    }

    int userMapSize = buffer.getInt();
    userMap = new HashMap<>(userMapSize);
    for (int i = 0; i < userMapSize; i++) {
      String userName = SerializeUtils.deserializeString(buffer);
      User user = new User();
      user.deserialize(buffer);
      userMap.put(userName, user);
    }

    int roleMapSize = buffer.getInt();
    roleMap = new HashMap<>(roleMapSize);
    for (int i = 0; i < roleMapSize; i++) {
      String userName = SerializeUtils.deserializeString(buffer);
      Role role = new Role();
      role.deserialize(buffer);
      roleMap.put(userName, role);
    }

    partitionTableBuffer = buffer;
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
    return Objects.equals(storageGroupTTLMap, snapshot.getStorageGroupTTLMap()) &&
        Objects.equals(userMap, snapshot.getUserMap()) &&
        Objects.equals(roleMap, snapshot.getRoleMap()) &&
        Objects.equals(partitionTableBuffer, snapshot.getPartitionTableBuffer());
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(super.hashCode(), storageGroupTTLMap, userMap, roleMap, partitionTableBuffer);
  }
}
