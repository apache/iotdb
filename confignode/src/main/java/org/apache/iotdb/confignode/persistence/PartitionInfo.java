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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The PartitionInfo stores cluster PartitionTable. The PartitionTable including: 1. regionMap:
 * location of Region member 2. schemaPartition: location of schema 3. dataPartition: location of
 * data
 */
public class PartitionInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionInfo.class);

  // Region read write lock
  private final ReentrantReadWriteLock regionReadWriteLock;
  private AtomicInteger nextRegionGroupId = new AtomicInteger(0);
  private final Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaMap;
  // Map<TConsensusGroupId, allocatedSlotsNumber>
  private final Map<TConsensusGroupId, Long> regionSlotsCounter;
  // preDeleted TODO: Combine it with Partition.class
  private final Set<String> preDeletedStorageGroup = new CopyOnWriteArraySet<>();
  private final Set<TRegionReplicaSet> deletedRegionSet = new HashSet<>();

  // SchemaPartition read write lock
  private final ReentrantReadWriteLock schemaPartitionReadWriteLock;
  private final SchemaPartition schemaPartition;

  // DataPartition read write lock
  private final ReentrantReadWriteLock dataPartitionReadWriteLock;
  private final DataPartition dataPartition;

  private final String snapshotFileName = "partition_info.bin";

  public PartitionInfo() {
    this.regionReadWriteLock = new ReentrantReadWriteLock();
    this.regionReplicaMap = new HashMap<>();
    this.regionSlotsCounter = new HashMap<>();

    this.schemaPartitionReadWriteLock = new ReentrantReadWriteLock();
    this.schemaPartition =
        new SchemaPartition(
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum());
    this.schemaPartition.setSchemaPartitionMap(new HashMap<>());

    this.dataPartitionReadWriteLock = new ReentrantReadWriteLock();
    this.dataPartition =
        new DataPartition(
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum());
    this.dataPartition.setDataPartitionMap(new HashMap<>());
  }

  public int generateNextRegionGroupId() {
    return nextRegionGroupId.getAndIncrement();
  }

  @TestOnly
  public Integer getNextRegionGroupId() {
    return nextRegionGroupId.get();
  }

  /**
   * Persistence allocation result of new Regions
   *
   * @param req CreateRegionsPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus createRegions(CreateRegionsReq req) {
    TSStatus result;
    regionReadWriteLock.writeLock().lock();
    try {
      int maxRegionId = Integer.MIN_VALUE;

      for (List<TRegionReplicaSet> regionReplicaSets : req.getRegionMap().values()) {
        for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
          regionReplicaMap.put(regionReplicaSet.getRegionId(), regionReplicaSet);
          regionSlotsCounter.put(regionReplicaSet.getRegionId(), 0L);
          maxRegionId = Math.max(maxRegionId, regionReplicaSet.getRegionId().getId());
        }
      }

      if (nextRegionGroupId.get() < maxRegionId) {
        // In this case, at least one Region is created with the leader node,
        // so the nextRegionGroupID of the followers needs to be added
        nextRegionGroupId.getAndAdd(req.getRegionMap().size());
      }

      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      regionReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Delete Regions
   *
   * @param req DeleteRegionsReq
   * @return SUCCESS_STATUS
   */
  public TSStatus deleteRegions(DeleteRegionsReq req) {
    TSStatus result;
    regionReadWriteLock.writeLock().lock();
    try {
      for (TConsensusGroupId consensusGroupId : req.getConsensusGroupIds()) {
        deletedRegionSet.add(regionReplicaMap.remove(consensusGroupId));
        regionSlotsCounter.remove(consensusGroupId);
      }
      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      regionReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Delete StorageGroup
   *
   * @param req DeleteRegionsReq
   */
  public void deleteStorageGroup(DeleteStorageGroupReq req) {
    TStorageGroupSchema storageGroupSchema = req.getStorageGroup();
    List<TConsensusGroupId> dataRegionGroupIds = storageGroupSchema.getDataRegionGroupIds();
    List<TConsensusGroupId> schemaRegionGroupIds = storageGroupSchema.getSchemaRegionGroupIds();
    DeleteRegionsReq deleteRegionsReq = new DeleteRegionsReq();
    for (TConsensusGroupId schemaRegionGroupId : schemaRegionGroupIds) {
      deleteRegionsReq.addConsensusGroupId(schemaRegionGroupId);
    }
    for (TConsensusGroupId dataRegionId : dataRegionGroupIds) {
      deleteRegionsReq.addConsensusGroupId(dataRegionId);
    }
    deleteRegions(deleteRegionsReq);
    deleteDataPartitionMapByStorageGroup(storageGroupSchema.getName());
    deleteSchemaPartitionMapByStorageGroup(storageGroupSchema.getName());
  }

  /**
   * Get SchemaPartition
   *
   * @param req SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(GetSchemaPartitionReq req) {
    SchemaPartitionResp schemaPartitionResp = new SchemaPartitionResp();
    schemaPartitionReadWriteLock.readLock().lock();

    try {
      schemaPartitionResp.setSchemaPartition(
          schemaPartition.getSchemaPartition(req.getPartitionSlotsMap(), preDeletedStorageGroup));
    } finally {
      schemaPartitionReadWriteLock.readLock().unlock();
      schemaPartitionResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }

    return schemaPartitionResp;
  }

  /**
   * Create SchemaPartition
   *
   * @param req CreateSchemaPartitionPlan with SchemaPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS when creation successful
   */
  public TSStatus createSchemaPartition(CreateSchemaPartitionReq req) {
    schemaPartitionReadWriteLock.writeLock().lock();
    regionReadWriteLock.writeLock().lock();

    try {
      // Allocate SchemaPartition by CreateSchemaPartitionPlan
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedResult =
          req.getAssignedSchemaPartition();
      assignedResult.forEach(
          (storageGroup, partitionSlots) ->
              partitionSlots.forEach(
                  (seriesPartitionSlot, regionReplicaSet) -> {
                    schemaPartition.createSchemaPartition(
                        storageGroup, seriesPartitionSlot, regionReplicaSet);
                    regionSlotsCounter.computeIfPresent(
                        regionReplicaSet.getRegionId(), (consensusGroupId, count) -> (count + 1));
                  }));
    } finally {
      regionReadWriteLock.writeLock().unlock();
      schemaPartitionReadWriteLock.writeLock().unlock();
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Filter no assigned SchemaPartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, List<TSeriesPartitionSlot>>
   * @return Map<StorageGroupName, List < TSeriesPartitionSlot>>, SchemaPartitionSlots that is not
   *     assigned in partitionSlotsMap
   */
  public Map<String, List<TSeriesPartitionSlot>> filterNoAssignedSchemaPartitionSlots(
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap) {
    Map<String, List<TSeriesPartitionSlot>> result;
    schemaPartitionReadWriteLock.readLock().lock();
    try {
      result = schemaPartition.filterNoAssignedSchemaPartitionSlot(partitionSlotsMap);
    } finally {
      schemaPartitionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * Get DataPartition
   *
   * @param req DataPartitionPlan with partitionSlotsMap
   * @return DataPartitionDataSet that contains only existing DataPartition
   */
  public DataSet getDataPartition(GetDataPartitionReq req) {
    DataPartitionResp dataPartitionResp = new DataPartitionResp();
    dataPartitionReadWriteLock.readLock().lock();

    try {
      dataPartitionResp.setDataPartition(
          dataPartition.getDataPartition(
              req.getPartitionSlotsMap(),
              ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
              ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum(),
              preDeletedStorageGroup));
    } finally {
      dataPartitionReadWriteLock.readLock().unlock();
      dataPartitionResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }

    return dataPartitionResp;
  }

  /**
   * Create DataPartition
   *
   * @param req CreateDataPartitionPlan with DataPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS when creation successful
   */
  public TSStatus createDataPartition(CreateDataPartitionReq req) {
    dataPartitionReadWriteLock.writeLock().lock();
    regionReadWriteLock.writeLock().lock();

    try {
      // Allocate DataPartition by CreateDataPartitionPlan
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          assignedResult = req.getAssignedDataPartition();
      assignedResult.forEach(
          (storageGroup, seriesPartitionTimePartitionSlots) ->
              seriesPartitionTimePartitionSlots.forEach(
                  ((seriesPartitionSlot, timePartitionSlotRegionReplicaSets) ->
                      timePartitionSlotRegionReplicaSets.forEach(
                          ((timePartitionSlot, regionReplicaSets) ->
                              regionReplicaSets.forEach(
                                  regionReplicaSet -> {
                                    dataPartition.createDataPartition(
                                        storageGroup,
                                        seriesPartitionSlot,
                                        timePartitionSlot,
                                        regionReplicaSet);
                                    regionSlotsCounter.computeIfPresent(
                                        regionReplicaSet.getRegionId(),
                                        (consensusGroupId, count) -> (count + 1));
                                  }))))));
    } finally {
      regionReadWriteLock.writeLock().unlock();
      dataPartitionReadWriteLock.writeLock().unlock();
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Filter no assigned DataPartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, Map<TSeriesPartitionSlot,
   *     List<TTimePartitionSlot>>>
   * @return Map<StorageGroupName, Map < TSeriesPartitionSlot, List < TTimePartitionSlot>>>,
   *     DataPartitionSlots that is not assigned in partitionSlotsMap
   */
  public Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
      filterNoAssignedDataPartitionSlots(
          Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> result;
    dataPartitionReadWriteLock.readLock().lock();
    try {
      result = dataPartition.filterNoAssignedDataPartitionSlots(partitionSlotsMap);
    } finally {
      dataPartitionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** Get RegionReplicaSet by the specific TConsensusGroupIds */
  public List<TRegionReplicaSet> getRegionReplicaSets(List<TConsensusGroupId> groupIds) {
    List<TRegionReplicaSet> result = new ArrayList<>();
    regionReadWriteLock.readLock().lock();
    try {
      for (TConsensusGroupId groupId : groupIds) {
        result.add(regionReplicaMap.get(groupId));
      }
    } finally {
      regionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** Get all allocated RegionReplicaSets */
  public List<TRegionReplicaSet> getAllocatedRegions() {
    List<TRegionReplicaSet> result;
    regionReadWriteLock.readLock().lock();
    try {
      result = new ArrayList<>(regionReplicaMap.values());
    } finally {
      regionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** @return A copy of regionReplicaMap */
  public Map<TConsensusGroupId, TRegionReplicaSet> getRegionReplicaMap() {
    Map<TConsensusGroupId, TRegionReplicaSet> result;
    regionReadWriteLock.readLock().lock();
    try {
      result = new HashMap<>(regionReplicaMap);
    } finally {
      regionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** @return The specific Regions that sorted by the number of allocated slots */
  public List<Pair<Long, TConsensusGroupId>> getSortedRegionSlotsCounter(
      List<TConsensusGroupId> consensusGroupIds) {
    List<Pair<Long, TConsensusGroupId>> result = new ArrayList<>();
    regionReadWriteLock.readLock().lock();
    try {
      for (TConsensusGroupId consensusGroupId : consensusGroupIds) {
        result.add(new Pair<>(regionSlotsCounter.get(consensusGroupId), consensusGroupId));
      }
      result.sort(Comparator.comparingLong(Pair::getLeft));
    } finally {
      regionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  private void deleteDataPartitionMapByStorageGroup(String storageGroup) {
    dataPartitionReadWriteLock.writeLock().lock();
    try {
      dataPartition.getDataPartitionMap().remove(storageGroup);
      preDeletedStorageGroup.remove(storageGroup);
    } finally {
      dataPartitionReadWriteLock.writeLock().unlock();
    }
  }

  private void deleteSchemaPartitionMapByStorageGroup(String storageGroup) {
    schemaPartitionReadWriteLock.writeLock().lock();
    try {
      schemaPartition.getSchemaPartitionMap().remove(storageGroup);
      preDeletedStorageGroup.remove(storageGroup);
    } finally {
      schemaPartitionReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus preDeleteStorageGroup(PreDeleteStorageGroupReq preDeleteStorageGroupReq) {
    final PreDeleteStorageGroupReq.PreDeleteType preDeleteType =
        preDeleteStorageGroupReq.getPreDeleteType();
    final String storageGroup = preDeleteStorageGroupReq.getStorageGroup();
    switch (preDeleteType) {
      case EXECUTE:
        preDeletedStorageGroup.add(storageGroup);
        break;
      case ROLLBACK:
        preDeletedStorageGroup.remove(storageGroup);
        break;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public Set<TRegionReplicaSet> getDeletedRegionSet() {
    return deletedRegionSet;
  }

  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {

    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    // prevents temporary files from being damaged and cannot be deleted, which affects the next
    // snapshot operation.
    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());

    lockAllRead();
    try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileOutputStream)) {
      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);

      // serialize nextRegionGroupId
      ReadWriteIOUtils.write(nextRegionGroupId.get(), fileOutputStream);
      // serialize regionMap
      serializeRegionMap(fileOutputStream, protocol);
      // serialize deletedRegionSet
      serializeDeletedRegionSet(fileOutputStream, protocol);
      // serialize schemaPartition
      schemaPartition.serialize(fileOutputStream, protocol);
      // serialize dataPartition
      dataPartition.serialize(fileOutputStream, protocol);
      // write to file
      fileOutputStream.flush();
      fileOutputStream.close();
      // rename file
      return tmpFile.renameTo(snapshotFile);
    } finally {
      unlockAllRead();
      // with or without success, delete temporary files anyway
      for (int retry = 0; retry < 5; retry++) {
        if (!tmpFile.exists() || tmpFile.delete()) {
          break;
        } else {
          LOGGER.warn(
              "Can't delete temporary snapshot file: {}, retrying...", tmpFile.getAbsolutePath());
        }
      }
    }
  }

  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {

    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    // no operations are processed at this time
    lockAllWrite();

    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileInputStream)) {
      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);
      // before restoring a snapshot, clear all old data
      clear();
      // start to restore
      nextRegionGroupId.set(ReadWriteIOUtils.readInt(fileInputStream));
      deserializeRegionMap(fileInputStream, protocol);
      // deserialize deletedRegionSet
      deserializeDeletedRegionSet(fileInputStream, protocol);
      schemaPartition.deserialize(fileInputStream, protocol);
      dataPartition.deserialize(fileInputStream, protocol);
    } finally {
      unlockAllWrite();
    }
  }

  /** Get SchemaNodeManagementPartition through matched storageGroup */
  public DataSet getSchemaNodeManagementPartition(List<String> matchedStorageGroups) {
    SchemaNodeManagementResp schemaNodeManagementResp = new SchemaNodeManagementResp();
    schemaPartitionReadWriteLock.readLock().lock();
    try {
      schemaNodeManagementResp.setSchemaPartition(
          schemaPartition.getSchemaPartition(matchedStorageGroups));
    } finally {
      schemaPartitionReadWriteLock.readLock().unlock();
      schemaNodeManagementResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }
    return schemaNodeManagementResp;
  }

  private void lockAllWrite() {
    regionReadWriteLock.writeLock().lock();
    schemaPartitionReadWriteLock.writeLock().lock();
    dataPartitionReadWriteLock.writeLock().lock();
  }

  private void unlockAllWrite() {
    regionReadWriteLock.writeLock().unlock();
    schemaPartitionReadWriteLock.writeLock().unlock();
    dataPartitionReadWriteLock.writeLock().unlock();
  }

  private void lockAllRead() {
    regionReadWriteLock.readLock().lock();
    schemaPartitionReadWriteLock.readLock().lock();
    dataPartitionReadWriteLock.readLock().lock();
  }

  private void unlockAllRead() {
    regionReadWriteLock.readLock().unlock();
    schemaPartitionReadWriteLock.readLock().unlock();
    dataPartitionReadWriteLock.readLock().unlock();
  }

  @TestOnly
  public DataPartition getDataPartition() {
    return dataPartition;
  }

  @TestOnly
  public SchemaPartition getSchemaPartition() {
    return schemaPartition;
  }

  @TestOnly
  public Map<TConsensusGroupId, Long> getRegionSlotsCounter() {
    return regionSlotsCounter;
  }

  private void serializeRegionMap(OutputStream outputStream, TProtocol protocol)
      throws TException, IOException {
    ReadWriteIOUtils.write(regionReplicaMap.size(), outputStream);
    for (TConsensusGroupId consensusGroupId : regionReplicaMap.keySet()) {
      consensusGroupId.write(protocol);
      regionReplicaMap.get(consensusGroupId).write(protocol);
      protocol.writeI64(regionSlotsCounter.get(consensusGroupId));
    }
  }

  private void deserializeRegionMap(InputStream inputStream, TProtocol protocol)
      throws TException, IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      TConsensusGroupId tConsensusGroupId = new TConsensusGroupId();
      tConsensusGroupId.read(protocol);
      TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();
      tRegionReplicaSet.read(protocol);
      Long count = protocol.readI64();

      regionReplicaMap.put(tConsensusGroupId, tRegionReplicaSet);
      regionSlotsCounter.put(tConsensusGroupId, count);
      size--;
    }
  }

  private void serializeDeletedRegionSet(OutputStream outputStream, TProtocol protocol)
      throws TException, IOException {
    ReadWriteIOUtils.write(regionReplicaMap.size(), outputStream);
    for (TRegionReplicaSet regionReplicaSet : deletedRegionSet) {
      regionReplicaSet.write(protocol);
    }
  }

  private void deserializeDeletedRegionSet(InputStream inputStream, TProtocol protocol)
      throws TException, IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();
      tRegionReplicaSet.read(protocol);
      deletedRegionSet.add(tRegionReplicaSet);
      size--;
    }
  }

  public void clear() {
    nextRegionGroupId = new AtomicInteger(0);
    regionReplicaMap.clear();
    regionSlotsCounter.clear();

    if (schemaPartition.getSchemaPartitionMap() != null) {
      schemaPartition.getSchemaPartitionMap().clear();
    }

    if (dataPartition.getDataPartitionMap() != null) {
      dataPartition.getDataPartitionMap().clear();
    }
  }
}
