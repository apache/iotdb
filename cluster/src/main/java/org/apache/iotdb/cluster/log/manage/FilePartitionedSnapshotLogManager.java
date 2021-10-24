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

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot.Factory;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Different from PartitionedSnapshotLogManager, FilePartitionedSnapshotLogManager does not store
 * the committed in memory after snapshots, it considers the logs are contained in the TsFiles so it
 * will record every TsFiles in the slot instead.
 */
public class FilePartitionedSnapshotLogManager extends PartitionedSnapshotLogManager<FileSnapshot> {

  private static final Logger logger =
      LoggerFactory.getLogger(FilePartitionedSnapshotLogManager.class);

  public FilePartitionedSnapshotLogManager(
      LogApplier logApplier,
      PartitionTable partitionTable,
      Node header,
      Node thisNode,
      DataGroupMember dataGroupMember) {
    super(logApplier, partitionTable, header, thisNode, Factory.INSTANCE, dataGroupMember);
  }

  /** send FlushPlan to all nodes in one dataGroup */
  private void syncFlushAllProcessor(List<Integer> requiredSlots, boolean needLeader) {
    logger.info("{}: Start flush all storage group processor in one data group", getName());
    Map<String, List<Pair<Long, Boolean>>> storageGroupPartitions =
        StorageEngine.getInstance().getWorkingStorageGroupPartitions();
    if (storageGroupPartitions.size() == 0) {
      logger.info("{}: no need to flush processor", getName());
      return;
    }
    dataGroupMember.flushFileWhenDoSnapshot(storageGroupPartitions, requiredSlots, needLeader);
  }

  @Override
  @SuppressWarnings("java:S1135") // ignore todos
  public void takeSnapshot() throws IOException {
    takeSnapshotForSpecificSlots(
        ((SlotPartitionTable) partitionTable).getNodeSlots(dataGroupMember.getHeader()), true);
  }

  @Override
  public void takeSnapshotForSpecificSlots(List<Integer> requiredSlots, boolean needLeader)
      throws IOException {
    try {
      logger.info("{}: Taking snapshots, flushing IoTDB", getName());
      // record current commit index and prevent further logs from being applied, so the
      // underlying state machine will not change during the snapshotting
      setBlockAppliedCommitIndex(getCommitLogIndex());
      // wait until all logs before BlockAppliedCommitIndex are applied
      super.takeSnapshot();
      // flush data to disk so that the disk files will represent a complete state
      syncFlushAllProcessor(requiredSlots, needLeader);
      logger.info("{}: Taking snapshots, IoTDB is flushed", getName());
      // TODO-cluster https://issues.apache.org/jira/browse/IOTDB-820
      synchronized (this) {
        collectTimeseriesSchemas(requiredSlots);
        snapshotLastLogIndex = getBlockAppliedCommitIndex();
        snapshotLastLogTerm = getTerm(snapshotLastLogIndex);
        collectTsFilesAndFillTimeseriesSchemas(requiredSlots);
        logger.info("{}: Snapshot is taken", getName());
      }
    } catch (EntryCompactedException e) {
      logger.error("failed to do snapshot.", e);
    } finally {
      // now further logs can be applied
      super.resetBlockAppliedCommitIndex();
    }
  }

  /**
   * IMPORTANT, separate the collection timeseries schema from tsfile to avoid the following
   * situations: If the tsfile is empty at this time (only the metadata is registered, but the
   * tsfile has not been written yet), then the timeseries schema snapshot can still be generated
   * and sent to the followers.
   *
   * @throws IOException
   */
  private void collectTsFilesAndFillTimeseriesSchemas(List<Integer> requiredSlots)
      throws IOException {
    // 1.collect tsfile
    collectTsFiles(requiredSlots);

    // 2.register the measurement
    boolean slotExistsInPartition;
    HashSet<Integer> slots = null;
    if (dataGroupMember.getMetaGroupMember() != null) {
      // if header node in raft group has removed, the result may be null
      List<Integer> nodeSlots =
          ((SlotPartitionTable) dataGroupMember.getMetaGroupMember().getPartitionTable())
              .getNodeSlots(dataGroupMember.getHeader());
      // the method of 'HashSet(Collection<? extends E> c)' throws NPE,so we need check this part
      if (nodeSlots != null) {
        slots = new HashSet<>(nodeSlots);
      }
    }

    for (Map.Entry<Integer, Collection<TimeseriesSchema>> entry : slotTimeseries.entrySet()) {
      int slotNum = entry.getKey();
      slotExistsInPartition = slots == null || slots.contains(slotNum);

      if (slotExistsInPartition) {
        FileSnapshot snapshot = slotSnapshots.computeIfAbsent(slotNum, s -> new FileSnapshot());
        if (snapshot.getTimeseriesSchemas().isEmpty()) {
          snapshot.setTimeseriesSchemas(entry.getValue());
        }
      }
    }
  }

  private void collectTsFiles(List<Integer> requiredSlots) throws IOException {
    slotSnapshots.clear();
    Map<PartialPath, Map<Long, List<TsFileResource>>> allClosedStorageGroupTsFile =
        StorageEngine.getInstance().getAllClosedStorageGroupTsFile();
    List<TsFileResource> createdHardlinks = new LinkedList<>();
    // group the TsFiles by their slots
    for (Entry<PartialPath, Map<Long, List<TsFileResource>>> entry :
        allClosedStorageGroupTsFile.entrySet()) {
      PartialPath storageGroupName = entry.getKey();
      Map<Long, List<TsFileResource>> storageGroupsFiles = entry.getValue();
      for (Entry<Long, List<TsFileResource>> storageGroupFiles : storageGroupsFiles.entrySet()) {
        Long partitionNum = storageGroupFiles.getKey();
        List<TsFileResource> resourceList = storageGroupFiles.getValue();
        if (!collectTsFiles(
            partitionNum, resourceList, storageGroupName, createdHardlinks, requiredSlots)) {
          // some file is deleted during the collecting, clean created hardlinks and restart
          // from the beginning
          for (TsFileResource createdHardlink : createdHardlinks) {
            createdHardlink.remove();
          }
          collectTsFiles(requiredSlots);
          return;
        }
      }
    }
  }

  /**
   * Create hardlinks for files in one partition and add them into the corresponding snapshot.
   *
   * @param partitionNum
   * @param resourceList
   * @param storageGroupName
   * @param createdHardlinks
   * @return true if all hardlinks are created successfully or false if some of them failed to
   *     create
   * @throws IOException
   */
  private boolean collectTsFiles(
      Long partitionNum,
      List<TsFileResource> resourceList,
      PartialPath storageGroupName,
      List<TsFileResource> createdHardlinks,
      List<Integer> requiredSlots)
      throws IOException {
    int slotNum =
        SlotPartitionTable.getSlotStrategy()
            .calculateSlotByPartitionNum(
                storageGroupName.getFullPath(), partitionNum, ClusterConstant.SLOT_NUM);
    if (!requiredSlots.contains(slotNum)) {
      return true;
    }
    FileSnapshot snapshot = slotSnapshots.computeIfAbsent(slotNum, s -> new FileSnapshot());
    for (TsFileResource tsFileResource : resourceList) {
      TsFileResource hardlink = tsFileResource.createHardlink();
      if (hardlink == null) {
        return false;
      }
      createdHardlinks.add(hardlink);
      logger.debug("{}: File {} is put into snapshot #{}", getName(), tsFileResource, slotNum);
      snapshot.addFile(hardlink, thisNode, isPlanIndexRangeUnique(tsFileResource, resourceList));
    }
    snapshot.getDataFiles().sort(Comparator.comparingLong(TsFileResource::getMaxPlanIndex));
    return true;
  }

  /**
   * Check if the plan index of 'resource' overlaps any one in 'others' from the same time
   * partition. For example, we have plan {1,2,3,4,5,6}, plan 1 and 6 are written into an unsequnce
   * file Unseq1， and {2,3} and {4,5} are written to sequence files Seq1 and Seq2 respectively
   * (notice the numbers are just indexes, not timestamps, so they can be written anywhere if
   * properly constructed). So Unseq1 both overlaps Seq1 and Seq2. If Unseq1 merges with Seq1 and
   * generated Seq1' (ranges [1, 6]), it will also overlap with Seq2. But if Seq1' further merge
   * with Seq2, its range remains to be [1,6], and we cannot find any other files that overlap with
   * it, so we can conclude with confidence that the file contains all plans within [1,6].
   *
   * @param resource
   * @param others
   * @return
   */
  private boolean isPlanIndexRangeUnique(TsFileResource resource, List<TsFileResource> others) {
    for (TsFileResource other : others) {
      if (other != resource && other.isPlanIndexOverlap(resource)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long append(Log entry) {
    long res = super.append(entry);
    // For data group, it's necessary to apply remove/add log immediately after append
    if (entry instanceof AddNodeLog || entry instanceof RemoveNodeLog) {
      applyEntry(entry);
    }
    return res;
  }
}
