/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.plan.analyze.cache;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.service.metrics.recorder.CacheMetricsRecorder;
import org.apache.iotdb.rpc.TSStatusCode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PartitionCache {
  private static final Logger logger = LoggerFactory.getLogger(PartitionCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final List<String> ROOT_PATH = Arrays.asList("root", "**");
  private static final String STORAGE_GROUP_CACHE_NAME = "Database";
  private static final String SCHEMA_PARTITION_CACHE_NAME = "SchemaPartition";
  private static final String DATA_PARTITION_CACHE_NAME = "DataPartition";

  /** calculate slotId by device */
  private final String seriesSlotExecutorName = config.getSeriesPartitionExecutorClass();

  private final int seriesPartitionSlotNum = config.getSeriesPartitionSlotNum();
  private final SeriesPartitionExecutor partitionExecutor;

  /** the size of partitionCache */
  private final int cacheSize = config.getPartitionCacheSize();
  /** the cache of database */
  private final Set<String> storageGroupCache = Collections.synchronizedSet(new HashSet<>());
  /** storage -> schemaPartitionTable */
  private final Cache<String, SchemaPartitionTable> schemaPartitionCache;
  /** storage -> dataPartitionTable */
  private final Cache<String, DataPartitionTable> dataPartitionCache;

  /** the latest time when groupIdToReplicaSetMap updated. */
  private final AtomicLong latestUpdateTime = new AtomicLong(0);
  /** TConsensusGroupId -> TRegionReplicaSet */
  private final Map<TConsensusGroupId, TRegionReplicaSet> groupIdToReplicaSetMap = new HashMap<>();

  /** The lock of cache */
  private final ReentrantReadWriteLock storageGroupCacheLock = new ReentrantReadWriteLock();

  private final ReentrantReadWriteLock schemaPartitionCacheLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock dataPartitionCacheLock = new ReentrantReadWriteLock();

  private final ReentrantReadWriteLock regionReplicaSetLock = new ReentrantReadWriteLock();

  private final IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager =
      ConfigNodeClientManager.getInstance();

  public PartitionCache() {
    this.schemaPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    this.dataPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    this.partitionExecutor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            this.seriesSlotExecutorName, this.seriesPartitionSlotNum);
  }

  // region database cache

  /**
   * get database to device map
   *
   * @param devicePaths the devices that need to hit
   * @param tryToFetch whether try to get all database from config node
   * @param isAutoCreate whether auto create database when cache miss
   */
  public Map<String, List<String>> getStorageGroupToDevice(
      List<String> devicePaths, boolean tryToFetch, boolean isAutoCreate) {
    StorageGroupCacheResult<List<String>> result =
        new StorageGroupCacheResult<List<String>>() {
          @Override
          public void put(String device, String storageGroupName) {
            map.computeIfAbsent(storageGroupName, k -> new ArrayList<>());
            map.get(storageGroupName).add(device);
          }
        };
    getStorageGroupCacheResult(result, devicePaths, tryToFetch, isAutoCreate);
    return result.getMap();
  }

  /**
   * get device to database map
   *
   * @param devicePaths the devices that need to hit
   * @param tryToFetch whether try to get all database from config node
   * @param isAutoCreate whether auto create database when cache miss
   */
  public Map<String, String> getDeviceToStorageGroup(
      List<String> devicePaths, boolean tryToFetch, boolean isAutoCreate) {
    StorageGroupCacheResult<String> result =
        new StorageGroupCacheResult<String>() {
          @Override
          public void put(String device, String storageGroupName) {
            map.put(device, storageGroupName);
          }
        };
    getStorageGroupCacheResult(result, devicePaths, tryToFetch, isAutoCreate);
    return result.getMap();
  }

  /**
   * get database of device
   *
   * @param devicePath the path of device
   * @return database name, return null if cache miss
   */
  private String getStorageGroupName(String devicePath) {
    synchronized (storageGroupCache) {
      for (String storageGroupName : storageGroupCache) {
        if (PathUtils.isStartWith(devicePath, storageGroupName)) {
          return storageGroupName;
        }
      }
    }
    return null;
  }

  /**
   * get all database from confignode and update database cache
   *
   * @param result the result of get database cache
   * @param devicePaths the devices that need to hit
   */
  private void fetchStorageGroupAndUpdateCache(
      StorageGroupCacheResult<?> result, List<String> devicePaths)
      throws ClientManagerException, TException {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      storageGroupCacheLock.writeLock().lock();
      result.reset();
      getStorageGroupMap(result, devicePaths, true);
      if (!result.isSuccess()) {
        TDatabaseSchemaResp storageGroupSchemaResp = client.getMatchedDatabaseSchemas(ROOT_PATH);
        if (storageGroupSchemaResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          Set<String> storageGroupNames = storageGroupSchemaResp.getDatabaseSchemaMap().keySet();
          // update all database into cache
          updateStorageCache(storageGroupNames);
        }
      }
    } finally {
      storageGroupCacheLock.writeLock().unlock();
    }
  }

  /**
   * create not existed database and update database cache
   *
   * @param result the result of get database cache
   * @param devicePaths the devices that need to hit
   * @throws RuntimeException if failed to create database
   */
  private void createStorageGroupAndUpdateCache(
      StorageGroupCacheResult<?> result, List<String> devicePaths)
      throws ClientManagerException, MetadataException, TException {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      storageGroupCacheLock.writeLock().lock();
      // try to check whether database need to be created
      result.reset();
      // try to hit database with all missed devices
      getStorageGroupMap(result, devicePaths, false);
      if (!result.isSuccess()) {
        // try to get database needed to be created from missed device
        Set<String> storageGroupNamesNeedCreated = new HashSet<>();
        for (String devicePath : result.getMissedDevices()) {
          PartialPath storageGroupNameNeedCreated =
              MetaUtils.getStorageGroupPathByLevel(
                  new PartialPath(devicePath), config.getDefaultStorageGroupLevel());
          storageGroupNamesNeedCreated.add(storageGroupNameNeedCreated.getFullPath());
        }

        // try to create databases one by one until done or one database fail
        Set<String> successFullyCreatedStorageGroup = new HashSet<>();
        for (String storageGroupName : storageGroupNamesNeedCreated) {
          TDatabaseSchema storageGroupSchema = new TDatabaseSchema();
          storageGroupSchema.setName(storageGroupName);
          TSStatus tsStatus = client.setDatabase(storageGroupSchema);
          if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == tsStatus.getCode()) {
            successFullyCreatedStorageGroup.add(storageGroupName);
          } else {
            // try to update cache by databases successfully created
            updateStorageCache(successFullyCreatedStorageGroup);
            logger.warn(
                "[{} Cache] failed to create database {}",
                STORAGE_GROUP_CACHE_NAME,
                storageGroupName);
            throw new RuntimeException(new IoTDBException(tsStatus.message, tsStatus.code));
          }
        }
        // try to update database cache when all databases has already been created
        updateStorageCache(storageGroupNamesNeedCreated);
      }
    } finally {
      storageGroupCacheLock.writeLock().unlock();
    }
  }

  /**
   * get database map in one try
   *
   * @param result contains result(boolean), failed devices and the map
   * @param devicePaths the devices that need to hit
   * @param failFast if true, return when failed. if false, return when all devices hit
   */
  private void getStorageGroupMap(
      StorageGroupCacheResult<?> result, List<String> devicePaths, boolean failFast) {
    try {
      storageGroupCacheLock.readLock().lock();
      // reset result before try
      result.reset();
      boolean status = true;
      for (String devicePath : devicePaths) {
        String storageGroupName = getStorageGroupName(devicePath);
        if (null == storageGroupName) {
          logger.debug(
              "[{} Cache] miss when search device {}", STORAGE_GROUP_CACHE_NAME, devicePath);
          status = false;
          if (failFast) {
            break;
          } else {
            result.addMissedDevice(devicePath);
          }
        } else {
          result.put(devicePath, storageGroupName);
        }
      }
      // setFailed the result when miss
      if (!status) {
        result.setFailed();
      }
      logger.debug("[{} Cache] hit when search device {}", STORAGE_GROUP_CACHE_NAME, devicePaths);
      CacheMetricsRecorder.record(status, STORAGE_GROUP_CACHE_NAME);
    } finally {
      storageGroupCacheLock.readLock().unlock();
    }
  }

  /**
   * get database map in three try
   *
   * @param result contains result, failed devices and map
   * @param devicePaths the devices that need to hit
   * @param tryToFetch whether try to get all database from confignode
   * @param isAutoCreate whether auto create database when device miss
   */
  private void getStorageGroupCacheResult(
      StorageGroupCacheResult<?> result,
      List<String> devicePaths,
      boolean tryToFetch,
      boolean isAutoCreate) {
    // miss when devicePath contains *
    for (String devicePath : devicePaths) {
      if (devicePath.contains("*")) {
        return;
      }
    }
    // first try to hit database in fast-fail way
    getStorageGroupMap(result, devicePaths, true);
    if (!result.isSuccess() && tryToFetch) {
      try {
        // try to fetch database from config node when miss
        fetchStorageGroupAndUpdateCache(result, devicePaths);
        // second try to hit database in fast-fail way
        getStorageGroupMap(result, devicePaths, true);
        if (!result.isSuccess() && isAutoCreate) {
          // try to auto create database of failed device
          createStorageGroupAndUpdateCache(result, devicePaths);
          // third try to hit database in fast-fail way
          getStorageGroupMap(result, devicePaths, true);
          if (!result.isSuccess()) {
            throw new StatementAnalyzeException("Failed to get database Map in three attempts.");
          }
        }
      } catch (TException | MetadataException | ClientManagerException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getDeviceToStorageGroup():" + e.getMessage());
      }
    }
  }

  /**
   * update database cache
   *
   * @param storageGroupNames the database names that need to update
   */
  public void updateStorageCache(Set<String> storageGroupNames) {
    storageGroupCacheLock.writeLock().lock();
    try {
      storageGroupCache.addAll(storageGroupNames);
    } finally {
      storageGroupCacheLock.writeLock().unlock();
    }
  }

  /**
   * invalidate database cache
   *
   * @param storageGroupNames the databases that need to invalid
   */
  public void removeFromStorageGroupCache(List<String> storageGroupNames) {
    storageGroupCacheLock.writeLock().lock();
    try {
      for (String storageGroupName : storageGroupNames) {
        storageGroupCache.remove(storageGroupName);
      }
    } finally {
      storageGroupCacheLock.writeLock().unlock();
    }
  }

  /** invalidate all database cache */
  public void removeFromStorageGroupCache() {
    storageGroupCacheLock.writeLock().lock();
    try {
      storageGroupCache.clear();
    } finally {
      storageGroupCacheLock.writeLock().unlock();
    }
  }

  // endregion

  // region replicaSet cache
  /**
   * get regionReplicaSet from local and confignode
   *
   * @param consensusGroupId the id of consensus group
   * @return regionReplicaSet
   * @throws RuntimeException if failed to get regionReplicaSet from confignode
   * @throws StatementAnalyzeException if there are exception when try to get latestRegionRouteMap
   */
  public TRegionReplicaSet getRegionReplicaSet(TConsensusGroupId consensusGroupId) {
    TRegionReplicaSet result;
    // try to get regionReplicaSet from cache
    try {
      regionReplicaSetLock.readLock().lock();
      result = groupIdToReplicaSetMap.get(consensusGroupId);
    } finally {
      regionReplicaSetLock.readLock().unlock();
    }
    if (result == null) {
      // if not hit then try to get regionReplicaSet from confignode
      try {
        regionReplicaSetLock.writeLock().lock();
        // verify that there are not hit in cache
        if (!groupIdToReplicaSetMap.containsKey(consensusGroupId)) {
          try (ConfigNodeClient client =
              configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
            TRegionRouteMapResp resp = client.getLatestRegionRouteMap();
            if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
              updateGroupIdToReplicaSetMap(resp.getTimestamp(), resp.getRegionRouteMap());
            }
            // if confignode don't have then will throw RuntimeException
            if (!groupIdToReplicaSetMap.containsKey(consensusGroupId)) {
              // failed to get RegionReplicaSet from confignode
              throw new RuntimeException(
                  "Failed to get replicaSet of consensus group[id= " + consensusGroupId + "]");
            }
          } catch (ClientManagerException | TException e) {
            throw new StatementAnalyzeException(
                "An error occurred when executing getRegionReplicaSet():" + e.getMessage());
          }
        }
        result = groupIdToReplicaSetMap.get(consensusGroupId);
      } finally {
        regionReplicaSetLock.writeLock().unlock();
      }
    }
    // try to get regionReplicaSet by consensusGroupId
    return result;
  }

  /**
   * update regionReplicaSetMap according to timestamp
   *
   * @param timestamp the timestamp of map that need to update
   * @param map consensusGroupId to regionReplicaSet map
   * @return true if update successfully or false when map is not latest
   */
  public boolean updateGroupIdToReplicaSetMap(
      long timestamp, Map<TConsensusGroupId, TRegionReplicaSet> map) {
    try {
      regionReplicaSetLock.writeLock().lock();
      boolean result = (timestamp == latestUpdateTime.accumulateAndGet(timestamp, Math::max));
      // if timestamp is greater than latestUpdateTime, then update
      if (result) {
        groupIdToReplicaSetMap.clear();
        groupIdToReplicaSetMap.putAll(map);
      }
      return result;
    } finally {
      regionReplicaSetLock.writeLock().unlock();
    }
  }

  /** invalidate replicaSetCache */
  public void invalidReplicaSetCache() {
    try {
      regionReplicaSetLock.writeLock().lock();
      groupIdToReplicaSetMap.clear();
    } finally {
      regionReplicaSetLock.writeLock().unlock();
    }
  }

  // endregion

  // region schema partition cache

  /**
   * get schemaPartition
   *
   * @param storageGroupToDeviceMap database to devices map
   * @return SchemaPartition of storageGroupToDeviceMap
   */
  public SchemaPartition getSchemaPartition(Map<String, List<String>> storageGroupToDeviceMap) {
    schemaPartitionCacheLock.readLock().lock();
    try {
      if (storageGroupToDeviceMap.size() == 0) {
        CacheMetricsRecorder.record(false, SCHEMA_PARTITION_CACHE_NAME);
        return null;
      }
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
          new HashMap<>();

      // check cache for each database
      for (Map.Entry<String, List<String>> entry : storageGroupToDeviceMap.entrySet()) {
        String storageGroupName = entry.getKey();
        Map<TSeriesPartitionSlot, TRegionReplicaSet> regionReplicaSetMap =
            schemaPartitionMap.computeIfAbsent(storageGroupName, k -> new HashMap<>());
        SchemaPartitionTable schemaPartitionTable =
            schemaPartitionCache.getIfPresent(storageGroupName);
        if (null == schemaPartitionTable) {
          // if database not find, then return cache miss.
          logger.debug(
              "[{} Cache] miss when search database {}",
              SCHEMA_PARTITION_CACHE_NAME,
              storageGroupName);
          CacheMetricsRecorder.record(false, SCHEMA_PARTITION_CACHE_NAME);
          return null;
        }
        Map<TSeriesPartitionSlot, TConsensusGroupId> map =
            schemaPartitionTable.getSchemaPartitionMap();
        // check cache for each device
        for (String device : entry.getValue()) {
          TSeriesPartitionSlot seriesPartitionSlot =
              partitionExecutor.getSeriesPartitionSlot(device);
          if (!map.containsKey(seriesPartitionSlot)) {
            // if one device not find, then return cache miss.
            logger.debug(
                "[{} Cache] miss when search device {}", SCHEMA_PARTITION_CACHE_NAME, device);
            CacheMetricsRecorder.record(false, SCHEMA_PARTITION_CACHE_NAME);
            return null;
          }
          TConsensusGroupId consensusGroupId = map.get(seriesPartitionSlot);
          TRegionReplicaSet regionReplicaSet = getRegionReplicaSet(consensusGroupId);
          regionReplicaSetMap.put(seriesPartitionSlot, regionReplicaSet);
        }
      }
      logger.debug("[{} Cache] hit", SCHEMA_PARTITION_CACHE_NAME);
      // cache hit
      CacheMetricsRecorder.record(true, SCHEMA_PARTITION_CACHE_NAME);
      return new SchemaPartition(
          schemaPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
    } finally {
      schemaPartitionCacheLock.readLock().unlock();
    }
  }

  /**
   * update schemaPartitionCache by schemaPartition.
   *
   * @param schemaPartitionTable database to SeriesPartitionSlot to ConsensusGroupId map
   */
  public void updateSchemaPartitionCache(
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable) {
    schemaPartitionCacheLock.writeLock().lock();
    try {
      for (Map.Entry<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> entry1 :
          schemaPartitionTable.entrySet()) {
        String storageGroupName = entry1.getKey();
        SchemaPartitionTable result = schemaPartitionCache.getIfPresent(storageGroupName);
        if (null == result) {
          result = new SchemaPartitionTable();
          schemaPartitionCache.put(storageGroupName, result);
        }
        Map<TSeriesPartitionSlot, TConsensusGroupId> seriesPartitionSlotTConsensusGroupIdMap =
            result.getSchemaPartitionMap();
        seriesPartitionSlotTConsensusGroupIdMap.putAll(entry1.getValue());
      }
    } finally {
      schemaPartitionCacheLock.writeLock().unlock();
    }
  }

  /**
   * invalid schemaPartitionCache by database
   *
   * @param storageGroupName the databases that need to invalid
   */
  public void invalidSchemaPartitionCache(String storageGroupName) {
    schemaPartitionCacheLock.writeLock().lock();
    try {
      schemaPartitionCache.invalidate(storageGroupName);
    } finally {
      schemaPartitionCacheLock.writeLock().unlock();
    }
  }

  /** invalid all schemaPartitionCache */
  public void invalidAllSchemaPartitionCache() {
    schemaPartitionCacheLock.writeLock().lock();
    try {
      schemaPartitionCache.invalidateAll();
    } finally {
      schemaPartitionCacheLock.writeLock().unlock();
    }
  }
  // endregion

  // region data partition cache

  /**
   * get dataPartition by query param map
   *
   * @param storageGroupToQueryParamsMap database to dataPartitionQueryParam map
   * @return DataPartition of storageGroupToQueryParamsMap
   */
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> storageGroupToQueryParamsMap) {
    dataPartitionCacheLock.readLock().lock();
    try {
      if (storageGroupToQueryParamsMap.size() == 0) {
        CacheMetricsRecorder.record(false, DATA_PARTITION_CACHE_NAME);
        return null;
      }
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap = new HashMap<>();
      // check cache for each database
      for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
          storageGroupToQueryParamsMap.entrySet()) {
        if (null == entry.getValue()
            || entry.getValue().isEmpty()
            || !getStorageGroupDataPartition(dataPartitionMap, entry.getKey(), entry.getValue())) {
          CacheMetricsRecorder.record(false, DATA_PARTITION_CACHE_NAME);
          return null;
        }
      }
      logger.debug("[{} Cache] hit", DATA_PARTITION_CACHE_NAME);
      // cache hit
      CacheMetricsRecorder.record(true, DATA_PARTITION_CACHE_NAME);
      return new DataPartition(dataPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
    } finally {
      dataPartitionCacheLock.readLock().unlock();
    }
  }

  /**
   * get dataPartition from database
   *
   * @param dataPartitionMap result
   * @param storageGroupName database that need to get
   * @param dataPartitionQueryParams specific query params of data partition
   * @return whether hit
   */
  private boolean getStorageGroupDataPartition(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap,
      String storageGroupName,
      List<DataPartitionQueryParam> dataPartitionQueryParams) {
    DataPartitionTable dataPartitionTable = dataPartitionCache.getIfPresent(storageGroupName);
    if (null == dataPartitionTable) {
      logger.debug(
          "[{} Cache] miss when search database {}", DATA_PARTITION_CACHE_NAME, storageGroupName);
      return false;
    }
    Map<TSeriesPartitionSlot, SeriesPartitionTable> cachedStorageGroupPartitionMap =
        dataPartitionTable.getDataPartitionMap();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
        seriesSlotToTimePartitionMap =
            dataPartitionMap.computeIfAbsent(storageGroupName, k -> new HashMap<>());
    // check cache for each device
    for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
      if (!getDeviceDataPartition(
          seriesSlotToTimePartitionMap, dataPartitionQueryParam, cachedStorageGroupPartitionMap)) {
        return false;
      }
    }
    return true;
  }

  /**
   * get dataPartition from device
   *
   * @param seriesSlotToTimePartitionMap result
   * @param dataPartitionQueryParam specific query param of data partition
   * @param cachedStorageGroupPartitionMap all cached data partition map of related database
   * @return whether hit
   */
  private boolean getDeviceDataPartition(
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          seriesSlotToTimePartitionMap,
      DataPartitionQueryParam dataPartitionQueryParam,
      Map<TSeriesPartitionSlot, SeriesPartitionTable> cachedStorageGroupPartitionMap) {
    TSeriesPartitionSlot seriesPartitionSlot;
    if (null != dataPartitionQueryParam.getDevicePath()) {
      seriesPartitionSlot =
          partitionExecutor.getSeriesPartitionSlot(dataPartitionQueryParam.getDevicePath());
    } else {
      return false;
    }
    SeriesPartitionTable cachedSeriesPartitionTable =
        cachedStorageGroupPartitionMap.get(seriesPartitionSlot);
    if (null == cachedSeriesPartitionTable) {
      logger.debug(
          "[{} Cache] miss when search device {}",
          DATA_PARTITION_CACHE_NAME,
          dataPartitionQueryParam.getDevicePath());
      return false;
    }
    Map<TTimePartitionSlot, List<TConsensusGroupId>> cachedTimePartitionSlot =
        cachedSeriesPartitionTable.getSeriesPartitionMap();
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotListMap =
        seriesSlotToTimePartitionMap.computeIfAbsent(seriesPartitionSlot, k -> new HashMap<>());
    // Notice: when query all time partition, then miss
    if (dataPartitionQueryParam.getTimePartitionSlotList().isEmpty()) {
      return false;
    }
    // check cache for each time partition
    for (TTimePartitionSlot timePartitionSlot :
        dataPartitionQueryParam.getTimePartitionSlotList()) {
      if (!getTimeSlotDataPartition(
          timePartitionSlotListMap, timePartitionSlot, cachedTimePartitionSlot)) {
        return false;
      }
    }
    return true;
  }

  /**
   * get dataPartition from time slot
   *
   * @param timePartitionSlotListMap result
   * @param timePartitionSlot the specific time partition slot of data partition
   * @param cachedTimePartitionSlot all cached time slot map of related device
   * @return whether hit
   */
  private boolean getTimeSlotDataPartition(
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotListMap,
      TTimePartitionSlot timePartitionSlot,
      Map<TTimePartitionSlot, List<TConsensusGroupId>> cachedTimePartitionSlot) {
    List<TConsensusGroupId> cacheConsensusGroupId = cachedTimePartitionSlot.get(timePartitionSlot);
    if (null == cacheConsensusGroupId
        || cacheConsensusGroupId.isEmpty()
        || null == timePartitionSlot) {
      logger.debug(
          "[{} Cache] miss when search time partition {}",
          DATA_PARTITION_CACHE_NAME,
          timePartitionSlot);
      return false;
    }
    List<TRegionReplicaSet> regionReplicaSets = new LinkedList<>();
    for (TConsensusGroupId consensusGroupId : cacheConsensusGroupId) {
      regionReplicaSets.add(getRegionReplicaSet(consensusGroupId));
    }
    timePartitionSlotListMap.put(timePartitionSlot, regionReplicaSets);
    return true;
  }

  /**
   * update dataPartitionCache by dataPartition
   *
   * @param dataPartitionTable database to seriesPartitionSlot to timePartitionSlot to
   *     ConsensusGroupId map
   */
  public void updateDataPartitionCache(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
          dataPartitionTable) {
    dataPartitionCacheLock.writeLock().lock();
    try {
      for (Map.Entry<
              String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
          entry1 : dataPartitionTable.entrySet()) {
        String storageGroupName = entry1.getKey();
        if (null != storageGroupName) {
          DataPartitionTable result = dataPartitionCache.getIfPresent(storageGroupName);
          boolean needToUpdateCache = (null == result);
          if (needToUpdateCache) {
            result = new DataPartitionTable();
          }
          Map<TSeriesPartitionSlot, SeriesPartitionTable>
              seriesPartitionSlotSeriesPartitionTableMap = result.getDataPartitionMap();
          for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
              entry2 : entry1.getValue().entrySet()) {
            TSeriesPartitionSlot seriesPartitionSlot = entry2.getKey();
            if (null != seriesPartitionSlot) {
              SeriesPartitionTable seriesPartitionTable;
              if (!seriesPartitionSlotSeriesPartitionTableMap.containsKey(seriesPartitionSlot)) {
                // if device not exists, then add new seriesPartitionTable
                seriesPartitionTable = new SeriesPartitionTable(entry2.getValue());
                seriesPartitionSlotSeriesPartitionTableMap.put(
                    seriesPartitionSlot, seriesPartitionTable);
              } else {
                // if device exists, then merge
                seriesPartitionTable =
                    seriesPartitionSlotSeriesPartitionTableMap.get(seriesPartitionSlot);
                Map<TTimePartitionSlot, List<TConsensusGroupId>> result3 =
                    seriesPartitionTable.getSeriesPartitionMap();
                result3.putAll(entry2.getValue());
              }
            }
          }
          if (needToUpdateCache) {
            dataPartitionCache.put(storageGroupName, result);
          }
        }
      }
    } finally {
      dataPartitionCacheLock.writeLock().unlock();
    }
  }

  /**
   * invalid dataPartitionCache by storageGroup
   *
   * @param storageGroup the databases that need to invalid
   */
  public void invalidDataPartitionCache(String storageGroup) {
    dataPartitionCacheLock.writeLock().lock();
    try {
      dataPartitionCache.invalidate(storageGroup);
    } finally {
      dataPartitionCacheLock.writeLock().unlock();
    }
  }

  /** invalid all dataPartitionCache */
  public void invalidAllDataPartitionCache() {
    dataPartitionCacheLock.writeLock().lock();
    try {
      dataPartitionCache.invalidateAll();
    } finally {
      dataPartitionCacheLock.writeLock().unlock();
    }
  }

  // endregion

  public void invalidAllCache() {
    logger.debug("[Partition Cache] invalid");
    removeFromStorageGroupCache();
    invalidAllDataPartitionCache();
    invalidAllSchemaPartitionCache();
    invalidReplicaSetCache();
    logger.debug("[Partition Cache] is invalid:{}", this);
  }

  @Override
  public String toString() {
    return "PartitionCache{"
        + "cacheSize="
        + cacheSize
        + ", storageGroupCache="
        + storageGroupCache
        + ", replicaSetCache="
        + groupIdToReplicaSetMap
        + ", schemaPartitionCache="
        + schemaPartitionCache
        + ", dataPartitionCache="
        + dataPartitionCache
        + '}';
  }
}
