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

import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.SnapshotInstallationException;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/** PartitionedSnapshot stores the snapshot of each slot in a map. */
public class PartitionedSnapshot<T extends Snapshot> extends Snapshot {

  private static final Logger logger = LoggerFactory.getLogger(PartitionedSnapshot.class);

  private Map<Integer, T> slotSnapshots;
  private SnapshotFactory<T> factory;

  public PartitionedSnapshot(SnapshotFactory<T> factory) {
    this(new HashMap<>(), factory);
  }

  private PartitionedSnapshot(Map<Integer, T> slotSnapshots, SnapshotFactory<T> factory) {
    this.slotSnapshots = slotSnapshots;
    this.factory = factory;
  }

  public void putSnapshot(int slot, T snapshot) {
    slotSnapshots.put(slot, snapshot);
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    logger.info("Start to serialize a snapshot of {} sub-snapshots", slotSnapshots.size());
    try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
      dataOutputStream.writeInt(slotSnapshots.size());
      for (Entry<Integer, T> entry : slotSnapshots.entrySet()) {
        dataOutputStream.writeInt(entry.getKey());
        dataOutputStream.write(entry.getValue().serialize().array());
      }
      dataOutputStream.writeLong(getLastLogIndex());
      dataOutputStream.writeLong(getLastLogTerm());
    } catch (IOException e) {
      // unreachable
    }

    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      int slot = buffer.getInt();
      T snapshot = factory.create();
      snapshot.deserialize(buffer);
      slotSnapshots.put(slot, snapshot);
    }
    setLastLogIndex(buffer.getLong());
    setLastLogTerm(buffer.getLong());
  }

  public T getSnapshot(int slot) {
    return slotSnapshots.get(slot);
  }

  @Override
  public String toString() {
    return "PartitionedSnapshot{"
        + "slotSnapshots="
        + slotSnapshots.size()
        + ", lastLogIndex="
        + lastLogIndex
        + ", lastLogTerm="
        + lastLogTerm
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionedSnapshot<?> snapshot = (PartitionedSnapshot<?>) o;
    return Objects.equals(slotSnapshots, snapshot.slotSnapshots);
  }

  @Override
  public SnapshotInstaller getDefaultInstaller(RaftMember member) {
    return new Installer((DataGroupMember) member);
  }

  @Override
  public int hashCode() {
    return Objects.hash(slotSnapshots);
  }

  @SuppressWarnings("java:S3740")
  public class Installer implements SnapshotInstaller<PartitionedSnapshot> {

    private DataGroupMember dataGroupMember;
    private String name;

    public Installer(DataGroupMember dataGroupMember) {
      this.dataGroupMember = dataGroupMember;
      this.name = dataGroupMember.getName();
    }

    @Override
    public void install(PartitionedSnapshot snapshot, int slot, boolean isDataMigration)
        throws SnapshotInstallationException {
      installPartitionedSnapshot(snapshot);
    }

    @Override
    public void install(Map<Integer, PartitionedSnapshot> snapshotMap, boolean isDataMigration) {
      throw new IllegalStateException("Method unimplemented");
    }

    /**
     * Install a PartitionedSnapshot, which is a slotNumber -> FileSnapshot map. Only the slots that
     * are managed by the the group will be applied. The lastLogId and lastLogTerm are also updated
     * according to the snapshot.
     *
     * @param snapshot
     */
    private void installPartitionedSnapshot(PartitionedSnapshot<T> snapshot)
        throws SnapshotInstallationException {
      logger.info(
          "{}: start to install a snapshot of {}-{}",
          dataGroupMember.getName(),
          snapshot.lastLogIndex,
          snapshot.lastLogTerm);
      synchronized (dataGroupMember.getSnapshotApplyLock()) {
        List<Integer> slots =
            ((SlotPartitionTable) dataGroupMember.getMetaGroupMember().getPartitionTable())
                .getNodeSlots(dataGroupMember.getHeader());
        for (Integer slot : slots) {
          T subSnapshot = snapshot.getSnapshot(slot);
          if (subSnapshot != null) {
            installSnapshot(subSnapshot, slot);
          }
        }
        synchronized (dataGroupMember.getLogManager()) {
          dataGroupMember.getLogManager().applySnapshot(snapshot);
        }
      }
    }

    /**
     * Apply a snapshot to the state machine, i.e., load the data and meta data contained in the
     * snapshot into the IoTDB instance. Currently the type of the snapshot should be ony
     * FileSnapshot, but more types may be supported in the future.
     *
     * @param snapshot
     */
    @SuppressWarnings("java:S1905") // cast is necessary
    void installSnapshot(T snapshot, int slot) throws SnapshotInstallationException {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: applying snapshot {}", name, snapshot);
      }
      // ensure storage groups are synchronized
      try {
        dataGroupMember.getMetaGroupMember().syncLeaderWithConsistencyCheck(true);
      } catch (CheckConsistencyException e) {
        throw new SnapshotInstallationException(e);
      }
      SnapshotInstaller<T> defaultInstaller =
          (SnapshotInstaller<T>) snapshot.getDefaultInstaller(dataGroupMember);
      defaultInstaller.install(snapshot, slot, false);
    }
  }

  @Override
  public void truncateBefore(long minIndex) {
    for (T value : slotSnapshots.values()) {
      value.truncateBefore(minIndex);
    }
  }
}
