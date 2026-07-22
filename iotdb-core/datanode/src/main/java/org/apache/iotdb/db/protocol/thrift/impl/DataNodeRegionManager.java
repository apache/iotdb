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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.log.LoggerPeriodicalLogReducer;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class takes the responsibility of managing regions, executing PlanNode of write type on
 * according regions and controlling the execution concurrency.
 */
public class DataNodeRegionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeRegionManager.class);

  private final SchemaEngine schemaEngine;
  private final StorageEngine storageEngine;

  private final Map<SchemaRegionId, ReentrantReadWriteLock> schemaRegionLockMap =
      new ConcurrentHashMap<>();
  private final Map<DataRegionId, ReentrantReadWriteLock> dataRegionLockMap =
      new ConcurrentHashMap<>();
  private final Object deletedRegionGroupFenceLock = new Object();
  private final BitSet deletedSchemaRegionGroups = new BitSet();
  private final BitSet deletedDataRegionGroups = new BitSet();
  private static final int REGION_CREATION_LOCK_COUNT = 256;
  private final ReentrantLock[] regionCreationLocks = new ReentrantLock[REGION_CREATION_LOCK_COUNT];

  private static class DataNodeRegionManagerHolder {
    private static final DataNodeRegionManager INSTANCE = new DataNodeRegionManager();

    private DataNodeRegionManagerHolder() {}
  }

  public static DataNodeRegionManager getInstance() {
    return DataNodeRegionManagerHolder.INSTANCE;
  }

  public void init() {
    schemaEngine
        .getAllSchemaRegions()
        .forEach(
            schemaRegion ->
                schemaRegionLockMap.put(
                    schemaRegion.getSchemaRegionId(), new ReentrantReadWriteLock(false)));

    storageEngine
        .getAllDataRegionIds()
        .forEach(
            dataRegionId -> dataRegionLockMap.put(dataRegionId, new ReentrantReadWriteLock(false)));
  }

  public void clear() {
    schemaRegionLockMap.clear();
    dataRegionLockMap.clear();
    synchronized (deletedRegionGroupFenceLock) {
      deletedSchemaRegionGroups.clear();
      deletedDataRegionGroups.clear();
    }
  }

  private DataNodeRegionManager() {
    this(SchemaEngine.getInstance(), StorageEngine.getInstance());
  }

  @TestOnly
  DataNodeRegionManager(SchemaEngine schemaEngine, StorageEngine storageEngine) {
    this.schemaEngine = schemaEngine;
    this.storageEngine = storageEngine;
    for (int i = 0; i < REGION_CREATION_LOCK_COUNT; i++) {
      regionCreationLocks[i] = new ReentrantLock();
    }
  }

  public ReentrantReadWriteLock getRegionLock(ConsensusGroupId consensusGroupId) {
    return consensusGroupId instanceof DataRegionId
        ? dataRegionLockMap.get(consensusGroupId)
        : schemaRegionLockMap.get(consensusGroupId);
  }

  public TSStatus createSchemaRegion(
      final TRegionReplicaSet regionReplicaSet, final String storageGroup) {
    TSStatus tsStatus;
    final SchemaRegionId schemaRegionId =
        new SchemaRegionId(regionReplicaSet.getRegionId().getId());
    final ReentrantLock creationLock = getRegionCreationLock(schemaRegionId);
    creationLock.lock();
    boolean localRegionExisted = true;
    boolean consensusGroupExisted = true;
    boolean localRegionCreated = false;
    try {
      if (isRegionGroupDeleted(schemaRegionId)) {
        return new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      }
      localRegionExisted = schemaEngine.getSchemaRegion(schemaRegionId) != null;
      consensusGroupExisted =
          SchemaRegionConsensusImpl.getInstance()
              .getAllConsensusGroupIds()
              .contains(schemaRegionId);
      localRegionCreated = schemaEngine.createSchemaRegionIfAbsent(storageGroup, schemaRegionId);
      final List<Peer> peers = new ArrayList<>();
      for (final TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        final TEndPoint endpoint =
            new TEndPoint(
                dataNodeLocation.getSchemaRegionConsensusEndPoint().getIp(),
                dataNodeLocation.getSchemaRegionConsensusEndPoint().getPort());
        peers.add(new Peer(schemaRegionId, dataNodeLocation.getDataNodeId(), endpoint));
      }
      SchemaRegionConsensusImpl.getInstance().createLocalPeer(schemaRegionId, peers);
      schemaRegionLockMap.putIfAbsent(schemaRegionId, new ReentrantReadWriteLock(false));
      tsStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final IllegalPathException e1) {
      LOGGER.error(DataNodeMiscMessages.CREATE_SCHEMA_REGION_FAILED_ILLEGAL_PATH, storageGroup);
      tsStatus = new TSStatus(TSStatusCode.ILLEGAL_PATH.getStatusCode());
      tsStatus.setMessage(DataNodeMiscMessages.CREATE_SCHEMA_REGION_FAILED_ILLEGAL_PATH_MSG);
    } catch (final MetadataException e2) {
      if (LoggerPeriodicalLogReducer.shouldLog(
          DataNodeMiscMessages.CREATE_SCHEMA_REGION_FAILED_FMT,
          e2.getClass().getName() + String.valueOf(e2.getMessage()))) {
        LOGGER.error(
            DataNodeMiscMessages.CREATE_SCHEMA_REGION_FAILED, storageGroup, e2.getMessage());
      }
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(
          String.format(DataNodeMiscMessages.CREATE_SCHEMA_REGION_FAILED_FMT, e2.getMessage()));
    } catch (final ConsensusGroupAlreadyExistException e) {
      schemaRegionLockMap.putIfAbsent(schemaRegionId, new ReentrantReadWriteLock(false));
      tsStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      tsStatus.setMessage(
          String.format(
              DataNodeMiscMessages.SCHEMA_REGION_ALREADY_EXISTS_FMT, schemaRegionId.getId()));
    } catch (final ConsensusException e) {
      rollbackSchemaRegionCreation(
          schemaRegionId, localRegionCreated, consensusGroupExisted, storageGroup);
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(e.getMessage());
    } catch (final RuntimeException | OutOfMemoryError e) {
      rollbackSchemaRegionCreation(
          schemaRegionId,
          localRegionCreated
              || (!localRegionExisted && schemaEngine.getSchemaRegion(schemaRegionId) != null),
          consensusGroupExisted,
          storageGroup);
      LOGGER.error(DataNodeMiscMessages.CREATE_SCHEMA_REGION_FAILED, storageGroup, e.getMessage());
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(
          String.format(DataNodeMiscMessages.CREATE_SCHEMA_REGION_FAILED_FMT, e.getMessage()));
    } finally {
      creationLock.unlock();
    }
    return tsStatus;
  }

  public TSStatus createDataRegion(TRegionReplicaSet regionReplicaSet, String storageGroup) {
    TSStatus tsStatus;
    DataRegionId dataRegionId = new DataRegionId(regionReplicaSet.getRegionId().getId());
    final ReentrantLock creationLock = getRegionCreationLock(dataRegionId);
    creationLock.lock();
    boolean localRegionExisted = true;
    boolean consensusGroupExisted = true;
    boolean localRegionCreated = false;
    try {
      if (isRegionGroupDeleted(dataRegionId)) {
        return new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      }
      localRegionExisted = storageEngine.getDataRegion(dataRegionId) != null;
      consensusGroupExisted =
          DataRegionConsensusImpl.getInstance().getAllConsensusGroupIds().contains(dataRegionId);
      localRegionCreated = storageEngine.createDataRegionIfAbsent(dataRegionId, storageGroup);
      List<Peer> peers = new ArrayList<>();
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        TEndPoint endpoint =
            new TEndPoint(
                dataNodeLocation.getDataRegionConsensusEndPoint().getIp(),
                dataNodeLocation.getDataRegionConsensusEndPoint().getPort());
        peers.add(new Peer(dataRegionId, dataNodeLocation.getDataNodeId(), endpoint));
      }
      DataRegionConsensusImpl.getInstance().createLocalPeer(dataRegionId, peers);
      dataRegionLockMap.putIfAbsent(dataRegionId, new ReentrantReadWriteLock(false));
      tsStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (DataRegionException e) {
      LOGGER.error(DataNodeMiscMessages.CREATE_DATA_REGION_FAILED, storageGroup, e.getMessage());
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(
          String.format(DataNodeMiscMessages.CREATE_DATA_REGION_FAILED_FMT, e.getMessage()));
    } catch (ConsensusGroupAlreadyExistException e) {
      dataRegionLockMap.putIfAbsent(dataRegionId, new ReentrantReadWriteLock(false));
      tsStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      tsStatus.setMessage(
          String.format(DataNodeMiscMessages.DATA_REGION_ALREADY_EXISTS_FMT, dataRegionId.getId()));
    } catch (ConsensusException e) {
      rollbackDataRegionCreation(
          dataRegionId, localRegionCreated, consensusGroupExisted, storageGroup);
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(e.getMessage());
    } catch (RuntimeException | OutOfMemoryError e) {
      rollbackDataRegionCreation(
          dataRegionId,
          localRegionCreated
              || (!localRegionExisted && storageEngine.getDataRegion(dataRegionId) != null),
          consensusGroupExisted,
          storageGroup);
      LOGGER.error(DataNodeMiscMessages.CREATE_DATA_REGION_FAILED, storageGroup, e.getMessage());
      tsStatus = new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      tsStatus.setMessage(
          String.format(DataNodeMiscMessages.CREATE_DATA_REGION_FAILED_FMT, e.getMessage()));
    } finally {
      creationLock.unlock();
    }
    return tsStatus;
  }

  private ReentrantLock getRegionCreationLock(ConsensusGroupId regionId) {
    return regionCreationLocks[
        (regionId.hashCode() & Integer.MAX_VALUE) % REGION_CREATION_LOCK_COUNT];
  }

  /**
   * Fences delayed create RPCs after a whole RegionGroup starts deletion.
   *
   * <p>RegionGroup ids are never reused. The caller acquires the same striped lock as creation, so
   * an already-running creation finishes before deletion starts, while every later creation is
   * rejected even if it came from an old ConfigNode leader.
   */
  public void markRegionGroupDeleted(ConsensusGroupId regionId) {
    final ReentrantLock creationLock = getRegionCreationLock(regionId);
    creationLock.lock();
    try {
      synchronized (deletedRegionGroupFenceLock) {
        getDeletedRegionGroupSet(regionId).set(regionId.getId());
      }
    } finally {
      creationLock.unlock();
    }
  }

  private boolean isRegionGroupDeleted(ConsensusGroupId regionId) {
    synchronized (deletedRegionGroupFenceLock) {
      return getDeletedRegionGroupSet(regionId).get(regionId.getId());
    }
  }

  private BitSet getDeletedRegionGroupSet(ConsensusGroupId regionId) {
    return regionId instanceof DataRegionId ? deletedDataRegionGroups : deletedSchemaRegionGroups;
  }

  private void rollbackDataRegionCreation(
      DataRegionId regionId,
      boolean localRegionCreated,
      boolean consensusGroupExisted,
      String storageGroup) {
    rollbackConsensusPeer(
        DataRegionConsensusImpl.getInstance(), regionId, consensusGroupExisted, storageGroup);
    if (localRegionCreated && !consensusGroupExisted) {
      storageEngine.deleteDataRegion(regionId);
      dataRegionLockMap.remove(regionId);
    }
  }

  private void rollbackSchemaRegionCreation(
      SchemaRegionId regionId,
      boolean localRegionCreated,
      boolean consensusGroupExisted,
      String storageGroup) {
    rollbackConsensusPeer(
        SchemaRegionConsensusImpl.getInstance(), regionId, consensusGroupExisted, storageGroup);
    if (localRegionCreated && !consensusGroupExisted) {
      try {
        schemaEngine.deleteSchemaRegion(regionId);
        schemaRegionLockMap.remove(regionId);
      } catch (MetadataException e) {
        LOGGER.error(
            DataNodeMiscMessages.CREATE_SCHEMA_REGION_FAILED, storageGroup, e.getMessage());
      }
    }
  }

  private void rollbackConsensusPeer(
      org.apache.iotdb.consensus.IConsensus consensus,
      ConsensusGroupId regionId,
      boolean consensusGroupExisted,
      String storageGroup) {
    if (consensusGroupExisted) {
      return;
    }
    try {
      if (consensus.getAllConsensusGroupIds().contains(regionId)) {
        consensus.deleteLocalPeer(regionId);
      }
    } catch (ConsensusException | RuntimeException | OutOfMemoryError e) {
      LOGGER.error(DataNodeMiscMessages.CREATE_DATA_REGION_FAILED, storageGroup, e.getMessage());
    }
  }

  public TSStatus createNewRegion(final ConsensusGroupId regionId, final String storageGroup) {
    final TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    LOGGER.info(DataNodeMiscMessages.START_CREATE_NEW_REGION, regionId);
    try {
      if (regionId instanceof DataRegionId) {
        final DataRegionId dataRegionId = (DataRegionId) regionId;
        storageEngine.createDataRegion(dataRegionId, storageGroup);
        dataRegionLockMap.putIfAbsent(dataRegionId, new ReentrantReadWriteLock(false));
      } else {
        SchemaRegionId schemaRegionId = (SchemaRegionId) regionId;
        schemaEngine.createSchemaRegion(storageGroup, schemaRegionId);
        schemaRegionLockMap.putIfAbsent(schemaRegionId, new ReentrantReadWriteLock(false));
      }
    } catch (final Exception e) {
      LOGGER.error(DataNodeMiscMessages.CREATE_NEW_REGION_ERROR, regionId, e);
      status.setCode(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          String.format(
              DataNodeMiscMessages.CREATE_NEW_REGION_ERROR_FMT, regionId, e.getMessage()));
      return status;
    }
    status.setMessage(String.format(DataNodeMiscMessages.CREATE_NEW_REGION_SUCCEED_FMT, regionId));
    LOGGER.info(DataNodeMiscMessages.SUCCEED_CREATE_NEW_REGION, regionId);
    return status;
  }

  public TSStatus deleteDataRegion(DataRegionId dataRegionId) {
    storageEngine.deleteDataRegion(dataRegionId);
    dataRegionLockMap.remove(dataRegionId);
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  public TSStatus deleteSchemaRegion(SchemaRegionId schemaRegionId) {
    try {
      schemaEngine.deleteSchemaRegion(schemaRegionId);
      PipeDataNodeAgent.runtime().schemaListener(schemaRegionId).close();
      schemaRegionLockMap.remove(schemaRegionId);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
    } catch (MetadataException e) {
      LOGGER.error(DataNodeMiscMessages.METADATA_ERROR, IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    }
  }
}
