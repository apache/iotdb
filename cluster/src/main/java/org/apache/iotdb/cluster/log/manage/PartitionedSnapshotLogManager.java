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

package org.apache.iotdb.cluster.log.manage;

import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.manage.serializable.SyncLogDequeSerializer;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.SnapshotFactory;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * PartitionedSnapshotLogManager provides a PartitionedSnapshot as snapshot, dividing each log to a
 * sub-snapshot according to its slot and stores timeseries schemas of each slot.
 */
public abstract class PartitionedSnapshotLogManager<T extends Snapshot> extends RaftLogManager {

  private static final Logger logger = LoggerFactory.getLogger(PartitionedSnapshotLogManager.class);

  Map<Integer, T> slotSnapshots = new HashMap<>();
  private SnapshotFactory<T> factory;
  Map<Integer, Collection<TimeseriesSchema>> slotTimeseries = new HashMap<>();
  long snapshotLastLogIndex;
  long snapshotLastLogTerm;
  PartitionTable partitionTable;
  Node thisNode;
  DataGroupMember dataGroupMember;

  protected PartitionedSnapshotLogManager(
      LogApplier logApplier,
      PartitionTable partitionTable,
      Node header,
      Node thisNode,
      SnapshotFactory<T> factory,
      DataGroupMember dataGroupMember) {
    super(new SyncLogDequeSerializer(header.nodeIdentifier), logApplier, header.toString());
    this.partitionTable = partitionTable;
    this.factory = factory;
    this.thisNode = thisNode;
    this.dataGroupMember = dataGroupMember;
  }

  @Override
  public Snapshot getSnapshot(long minIndex) {
    // copy snapshots
    synchronized (slotSnapshots) {
      PartitionedSnapshot<T> partitionedSnapshot = new PartitionedSnapshot<>(factory);
      for (Entry<Integer, T> entry : slotSnapshots.entrySet()) {
        partitionedSnapshot.putSnapshot(entry.getKey(), entry.getValue());
      }
      partitionedSnapshot.setLastLogIndex(snapshotLastLogIndex);
      partitionedSnapshot.setLastLogTerm(snapshotLastLogTerm);
      partitionedSnapshot.truncateBefore(minIndex);
      return partitionedSnapshot;
    }
  }

  void collectTimeseriesSchemas() {
    slotTimeseries.clear();
    List<StorageGroupMNode> allSgNodes = IoTDB.metaManager.getAllStorageGroupNodes();
    boolean slotExistsInPartition;
    List<Integer> slots = null;
    if (dataGroupMember.getMetaGroupMember() != null) {
      slots =
          ((SlotPartitionTable) dataGroupMember.getMetaGroupMember().getPartitionTable())
              .getNodeSlots(dataGroupMember.getHeader());
    }
    for (MNode sgNode : allSgNodes) {
      String storageGroupName = sgNode.getFullPath();
      int slot =
          SlotPartitionTable.getSlotStrategy()
              .calculateSlotByTime(
                  storageGroupName, 0, ((SlotPartitionTable) partitionTable).getTotalSlotNumbers());
      slotExistsInPartition = slots == null || slots.contains(slot);

      if (slotExistsInPartition) {
        Collection<TimeseriesSchema> schemas =
            slotTimeseries.computeIfAbsent(slot, s -> new HashSet<>());
        IoTDB.metaManager.collectTimeseriesSchema(sgNode, schemas);
        logger.debug("{}: {} timeseries are snapshot in slot {}", getName(), schemas.size(), slot);
      }
    }
  }
}
