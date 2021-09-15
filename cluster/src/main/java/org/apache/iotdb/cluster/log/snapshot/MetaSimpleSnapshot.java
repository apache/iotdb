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

import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SerializeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** MetaSimpleSnapshot also records all storage groups. */
public class MetaSimpleSnapshot extends Snapshot {

  private static final Logger logger = LoggerFactory.getLogger(MetaSimpleSnapshot.class);
  private Map<PartialPath, Long> storageGroupTTLMap;
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
      Map<PartialPath, Long> storageGroupTTLMap,
      Map<String, User> userMap,
      Map<String, Role> roleMap,
      ByteBuffer partitionTableBuffer) {
    this.storageGroupTTLMap = storageGroupTTLMap;
    this.userMap = userMap;
    this.roleMap = roleMap;
    this.partitionTableBuffer = partitionTableBuffer;
  }

  public Map<PartialPath, Long> getStorageGroupTTLMap() {
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
      for (Map.Entry<PartialPath, Long> entry : storageGroupTTLMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey().getFullPath(), dataOutputStream);
        dataOutputStream.writeLong(entry.getValue());
      }

      dataOutputStream.writeInt(userMap.size());
      for (Map.Entry<String, User> entry : userMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        logger.info("A user into snapshot: {}", entry.getValue());
        dataOutputStream.write(entry.getValue().serialize().array());
      }

      dataOutputStream.writeInt(roleMap.size());
      for (Map.Entry<String, Role> entry : roleMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        logger.info("A role into snapshot: {}", entry.getValue());
        dataOutputStream.write(entry.getValue().serialize().array());
      }

      dataOutputStream.writeLong(lastLogIndex);
      dataOutputStream.writeLong(lastLogTerm);

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
      try {
        storageGroupTTLMap.put(
            new PartialPath(SerializeUtils.deserializeString(buffer)), buffer.getLong());
      } catch (IllegalPathException e) {
        // ignore
      }
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

    setLastLogIndex(buffer.getLong());
    setLastLogTerm(buffer.getLong());

    partitionTableBuffer = buffer;
  }

  @Override
  public SnapshotInstaller getDefaultInstaller(RaftMember member) {
    return new Installer((MetaGroupMember) member);
  }

  public static class Installer implements SnapshotInstaller<MetaSimpleSnapshot> {

    private static final Logger logger = LoggerFactory.getLogger(Installer.class);
    private MetaGroupMember metaGroupMember;

    public Installer(MetaGroupMember metaGroupMember) {
      this.metaGroupMember = metaGroupMember;
    }

    @Override
    public void install(MetaSimpleSnapshot snapshot, int slot, boolean isDataMigration) {
      installSnapshot(snapshot);
    }

    @Override
    public void install(Map<Integer, MetaSimpleSnapshot> snapshotMap, boolean isDataMigration) {
      throw new UnsupportedOperationException("Method unimplemented");
    }

    /**
     * Install a meta snapshot to IoTDB. The snapshot contains: all storage groups, partition table,
     * authentication info, and last log term/index in the snapshot.
     */
    private void installSnapshot(MetaSimpleSnapshot snapshot) {
      synchronized (metaGroupMember.getSnapshotApplyLock()) {
        // 1.  register all storage groups
        for (Map.Entry<PartialPath, Long> entry : snapshot.getStorageGroupTTLMap().entrySet()) {
          PartialPath sgPath = entry.getKey();
          try {
            IoTDB.metaManager.setStorageGroup(sgPath);
          } catch (StorageGroupAlreadySetException e) {
            // ignore
          } catch (MetadataException e) {
            logger.error(
                "{}: Cannot add storage group {} in snapshot, errMessage:{}",
                metaGroupMember.getName(),
                entry.getKey(),
                e.getMessage());
          }

          // 2. register ttl in the snapshot
          try {
            IoTDB.metaManager.setTTL(sgPath, entry.getValue());
            StorageEngine.getInstance().setTTL(sgPath, entry.getValue());
          } catch (MetadataException | IOException e) {
            logger.error(
                "{}: Cannot set ttl in storage group {} , errMessage: {}",
                metaGroupMember.getName(),
                entry.getKey(),
                e.getMessage());
          }
        }

        // 3. replace all users and roles
        try {
          IAuthorizer authorizer = BasicAuthorizer.getInstance();
          installSnapshotUsers(authorizer, snapshot);
          installSnapshotRoles(authorizer, snapshot);
        } catch (AuthException e) {
          logger.error(
              "{}: Cannot get authorizer instance, error is: ", metaGroupMember.getName(), e);
        }

        // 4. accept partition table
        metaGroupMember.acceptPartitionTable(snapshot.getPartitionTableBuffer(), true);

        synchronized (metaGroupMember.getLogManager()) {
          metaGroupMember.getLogManager().applySnapshot(snapshot);
        }
      }
    }

    private void installSnapshotUsers(IAuthorizer authorizer, MetaSimpleSnapshot snapshot) {
      try {
        authorizer.replaceAllUsers(snapshot.getUserMap());
      } catch (AuthException e) {
        logger.error("{}:replace users failed", metaGroupMember.getName(), e);
      }
    }

    private void installSnapshotRoles(IAuthorizer authorizer, MetaSimpleSnapshot snapshot) {
      try {
        authorizer.replaceAllRoles(snapshot.getRoleMap());
      } catch (AuthException e) {
        logger.error("{}:replace roles failed", metaGroupMember.getName(), e);
      }
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
    MetaSimpleSnapshot that = (MetaSimpleSnapshot) o;
    return Objects.equals(storageGroupTTLMap, that.storageGroupTTLMap)
        && Objects.equals(userMap, that.userMap)
        && Objects.equals(roleMap, that.roleMap)
        && Objects.equals(partitionTableBuffer, that.partitionTableBuffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageGroupTTLMap, userMap, roleMap, partitionTableBuffer);
  }

  public static class Factory implements SnapshotFactory<MetaSimpleSnapshot> {

    public static final FileSnapshot.Factory INSTANCE = new FileSnapshot.Factory();

    @Override
    public MetaSimpleSnapshot create() {
      return new MetaSimpleSnapshot();
    }

    @Override
    public MetaSimpleSnapshot copy(MetaSimpleSnapshot origin) {
      MetaSimpleSnapshot metaSimpleSnapshot = create();
      metaSimpleSnapshot.lastLogIndex = origin.lastLogIndex;
      metaSimpleSnapshot.lastLogTerm = origin.lastLogTerm;
      metaSimpleSnapshot.partitionTableBuffer = origin.partitionTableBuffer.duplicate();
      metaSimpleSnapshot.roleMap = new HashMap<>(origin.roleMap);
      metaSimpleSnapshot.userMap = new HashMap<>(origin.userMap);
      metaSimpleSnapshot.storageGroupTTLMap = new HashMap<>(origin.storageGroupTTLMap);
      return metaSimpleSnapshot;
    }
  }
}
