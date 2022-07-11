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
import org.apache.iotdb.commons.consensus.PartitionRegionId;
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
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PartitionCache {
  private static final Logger logger = LoggerFactory.getLogger(PartitionCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final List<String> ROOT_PATH = Arrays.asList("root", "**");
  private static final String STORAGE_GROUP_CACHE_NAME = "StorageGroup";
  private static final String SCHEMA_PARTITION_CACHE_NAME = "SchemaPartition";
  private static final String DATA_PARTITION_CACHE_NAME = "DataPartition";

  /** calculate slotId by device */
  private final String seriesSlotExecutorName = config.getSeriesPartitionExecutorClass();

  private final int seriesPartitionSlotNum = config.getSeriesPartitionSlotNum();
  private final SeriesPartitionExecutor partitionExecutor;

  /** the size of partitionCache */
  private final int cacheSize = config.getPartitionCacheSize();
  /** the cache of storage group */
  private final Set<String> storageGroupCache = Collections.synchronizedSet(new HashSet<>());
  /** storage -> schemaPartitionTable */
  private final Cache<String, SchemaPartitionTable> schemaPartitionCache;
  /** storage -> dataPartitionTable */
  private final Cache<String, DataPartitionTable> dataPartitionCache;

  /** the latest time when groupIdToReplicaSetMap updated. */
  private final AtomicLong latestUpdateTime = new AtomicLong(0);
  /** TConsensusGroupId -> TRegionReplicaSet */
  private final Map<TConsensusGroupId, TRegionReplicaSet> groupIdToReplicaSetMap =
      new ConcurrentHashMap<>();

  /** The lock of cache */
  private final ReentrantReadWriteLock storageGroupCacheLock = new ReentrantReadWriteLock();

  private final ReentrantReadWriteLock schemaPartitionCacheLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock dataPartitionCacheLock = new ReentrantReadWriteLock();

  private final IClientManager<PartitionRegionId, ConfigNodeClient> configNodeClientManager =
      new IClientManager.Factory<PartitionRegionId, ConfigNodeClient>()
          .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());

  public PartitionCache() {
    this.schemaPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    this.dataPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    this.partitionExecutor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            this.seriesSlotExecutorName, this.seriesPartitionSlotNum);
  }

  // region storage group cache

  /**
   * get storage group to device map
   *
   * @param devicePaths the devices that need to match
   * @param isAutoCreate whether auto create storage group when device miss
   */
  public Map<String, List<String>> getStorageGroupToDevice(
      List<String> devicePaths, boolean isAutoCreate) {
    StorageGroupCacheResult<List<String>> result =
        new StorageGroupCacheResult<List<String>>() {
          @Override
          public void put(String device, String storageGroup) {
            map.computeIfAbsent(storageGroup, k -> new ArrayList<>());
            map.get(storageGroup).add(device);
          }
        };
    getStorageGroupCacheResult(result, devicePaths, isAutoCreate);
    return result.getMap();
  }

  /**
   * get device to storage group map
   *
   * @param devicePaths the devices that need to match
   * @param isAutoCreate whether auto create storage group when device miss
   */
  public Map<String, String> getDeviceToStorageGroup(
      List<String> devicePaths, boolean isAutoCreate) {
    StorageGroupCacheResult<String> result =
        new StorageGroupCacheResult<String>() {
          @Override
          public void put(String device, String storageGroup) {
            map.put(device, storageGroup);
          }
        };
    getStorageGroupCacheResult(result, devicePaths, isAutoCreate);
    return result.getMap();
  }

  /**
   * get storage group of device
   *
   * @param devicePath the path of device
   * @return storage group. return null if cache miss
   */
  private String getStorageGroup(String devicePath) {
    synchronized (storageGroupCache) {
      for (String storageGroup : storageGroupCache) {
        if (PathUtils.isStartWith(devicePath, storageGroup)) {
          return storageGroup;
        }
      }
    }
    return null;
  }

  /** get all storage group from confignode and update storage group cache */
  private void fetchStorageGroupAndUpdateCache() throws IOException, TException {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TStorageGroupSchemaResp storageGroupSchemaResp =
          client.getMatchedStorageGroupSchemas(ROOT_PATH);
      if (storageGroupSchemaResp.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        Set<String> storageGroupNames = storageGroupSchemaResp.getStorageGroupSchemaMap().keySet();
        // update all storage group into cache
        updateStorageCache(storageGroupNames);
      }
    }
  }

  /** create not existed storage group and update storage group cache */
  private void createStorageGroupAndUpdateCache(Set<String> storageGroupNamesNeedCreated)
      throws IOException, TException {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      storageGroupCacheLock.writeLock().lock();
      Set<String> successFullyCreatedStorageGroup = new HashSet<>();
      for (String storageGroupName : storageGroupNamesNeedCreated) {
        TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
        storageGroupSchema.setName(storageGroupName);
        TSetStorageGroupReq req = new TSetStorageGroupReq(storageGroupSchema);
        TSStatus tsStatus = client.setStorageGroup(req);
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == tsStatus.getCode()) {
          successFullyCreatedStorageGroup.add(storageGroupName);
        } else {
          updateStorageCache(successFullyCreatedStorageGroup);
          logger.warn(
              "[{} Cache] failed to create storage group {}",
              STORAGE_GROUP_CACHE_NAME,
              storageGroupName);
          throw new RuntimeException(new IoTDBException(tsStatus.message, tsStatus.code));
        }
      }
      updateStorageCache(storageGroupNamesNeedCreated);
    } finally {
      storageGroupCacheLock.writeLock().unlock();
    }
  }

  /**
   * get storage group map
   *
   * @param result contains result(boolean), failed devices and the map
   * @param devicePaths the devices that need to match
   * @param failFast if true, return when failed. if false, return when all devices finish match
   */
  private void getStorageGroupMap(
      StorageGroupCacheResult<?> result, List<String> devicePaths, boolean failFast) {
    try {
      result.reset();
      storageGroupCacheLock.readLock().lock();
      boolean res = true;
      for (String devicePath : devicePaths) {
        String storageGroup = getStorageGroup(devicePath);
        if (null == storageGroup) {
          logger.debug(
              "[{} Cache] miss when search device {}", STORAGE_GROUP_CACHE_NAME, devicePath);
          res = false;
          if (failFast) {
            break;
          } else {
            result.addFailedDevice(devicePath);
          }
        } else {
          result.put(devicePath, storageGroup);
        }
      }
      if (!res) {
        result.clear();
      }
      logger.debug("[{} Cache] hit when search device {}", STORAGE_GROUP_CACHE_NAME, devicePaths);
      CacheMetricsRecorder.record(res, STORAGE_GROUP_CACHE_NAME);
    } finally {
      storageGroupCacheLock.readLock().unlock();
    }
  }

  /**
   * get storage group map in three try
   *
   * @param result contains result(boolean), failed devices and the map
   * @param devicePaths the devices that need to match
   * @param isAutoCreate whether auto create storage group when device miss
   */
  private void getStorageGroupCacheResult(
      StorageGroupCacheResult<?> result, List<String> devicePaths, boolean isAutoCreate) {
    Map<String, String> deviceToStorageGroupMap = new HashMap<>();
    // miss when devicePath contains *
    for (String devicePath : devicePaths) {
      if (devicePath.contains("*")) {
        return;
      }
    }
    // first try to hit storage group in fast-fail way
    getStorageGroupMap(result, devicePaths, true);
    if (!result.isSuccess()) {
      try {
        // when local cache not have, then try to fetch all storage group from config node
        fetchStorageGroupAndUpdateCache();
        // second try to hit storage group with failed devices;
        getStorageGroupMap(result, devicePaths, false);
        if (!result.isSuccess() && isAutoCreate) {
          // try to auto create storage group of failed device
          Set<String> storageGroupNamesNeedCreated = new HashSet<>();
          for (String devicePath : result.getFailedDevices()) {
            PartialPath storageGroupNameNeedCreated =
                MetaUtils.getStorageGroupPathByLevel(
                    new PartialPath(devicePath), config.getDefaultStorageGroupLevel());
            storageGroupNamesNeedCreated.add(storageGroupNameNeedCreated.getFullPath());
          }
          createStorageGroupAndUpdateCache(storageGroupNamesNeedCreated);
          // third try to hit cache
          getStorageGroupMap(result, devicePaths, false);
          if (!result.isSuccess()) {
            throw new StatementAnalyzeException("Failed to get Storage Group Map in three try.");
          }
        }
      } catch (TException | MetadataException | IOException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getDeviceToStorageGroup():" + e.getMessage());
      }
    }
  }

  /**
   * update storage group cache
   *
   * @param storageGroupNames the storage groups that need to update
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
   * invalid storage group cache
   *
   * @param storageGroupNames the storage groups that need to invalid
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

  /** invalid all storage group cache */
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
   * get regionReplicaSet. if groupIdToReplicaSetMap contains consensusGroupId then return, else it
   * will call getLatestRegionRouteMap from confignode and retry
   *
   * @param consensusGroupId the id of consensus group
   * @return regionReplicaSet
   * @throws RuntimeException if regionReplicaSet is null
   * @throws StatementAnalyzeException if there are exception when try to get latestRegionRouteMap
   */
  public TRegionReplicaSet getRegionReplicaSet(TConsensusGroupId consensusGroupId) {
    if (!groupIdToReplicaSetMap.containsKey(consensusGroupId)) {
      synchronized (groupIdToReplicaSetMap) {
        try (ConfigNodeClient client =
            configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
          TRegionRouteMapResp resp = client.getLatestRegionRouteMap();
          if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
            updateGroupIdToReplicaSetMap(resp.getTimestamp(), resp.getRegionRouteMap());
          }
          if (!groupIdToReplicaSetMap.containsKey(consensusGroupId)) {
            throw new RuntimeException(
                "Failed to get replicaSet of consensus group[id= " + consensusGroupId + "]");
          }
        } catch (IOException | TException e) {
          throw new StatementAnalyzeException(
              "An error occurred when executing getRegionReplicaSet():" + e.getMessage());
        }
      }
    }
    return groupIdToReplicaSetMap.get(consensusGroupId);
  }

  /**
   * update regionReplicaSetMap according to timestamp
   *
   * @param timestamp the timestamp of map that need to update
   * @param map consensusGroupId to regionReplicaSet map
   * @return true if update successfully or false when is not latest map
   */
  public boolean updateGroupIdToReplicaSetMap(
      long timestamp, Map<TConsensusGroupId, TRegionReplicaSet> map) {
    boolean result = (timestamp == latestUpdateTime.accumulateAndGet(timestamp, Math::max));
    // if timestamp is greater than latestUpdateTime, then update
    if (result) {
      groupIdToReplicaSetMap.clear();
      groupIdToReplicaSetMap.putAll(map);
    }
    return result;
  }

  /** invalid all replica Cache */
  public void invalidReplicaSetCache() {
    groupIdToReplicaSetMap.clear();
  }

  // endregion

  // region schema partition cache

  /**
   * get schemaPartition
   *
   * @param storageGroupToDeviceMap storage group to devices map
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

      // check cache for each storage group
      for (Map.Entry<String, List<String>> entry : storageGroupToDeviceMap.entrySet()) {
        String storageGroup = entry.getKey();
        Map<TSeriesPartitionSlot, TRegionReplicaSet> regionReplicaSetMap =
            schemaPartitionMap.computeIfAbsent(storageGroup, k -> new HashMap<>());
        SchemaPartitionTable schemaPartitionTable = schemaPartitionCache.getIfPresent(storageGroup);
        if (null == schemaPartitionTable) {
          // if storage group not find, then return cache miss.
          logger.debug(
              "[{} Cache] miss when search storage group {}",
              SCHEMA_PARTITION_CACHE_NAME,
              storageGroup);
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
   * @param schemaPartitionTable storage group to SeriesPartitionSlot to ConsensusGroupId map
   */
  public void updateSchemaPartitionCache(
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable) {
    schemaPartitionCacheLock.writeLock().lock();
    try {
      for (Map.Entry<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> entry1 :
          schemaPartitionTable.entrySet()) {
        String storageGroup = entry1.getKey();
        SchemaPartitionTable result = schemaPartitionCache.getIfPresent(storageGroup);
        if (null == result) {
          result = new SchemaPartitionTable();
          schemaPartitionCache.put(storageGroup, result);
        }
        Map<TSeriesPartitionSlot, TConsensusGroupId> result2 = result.getSchemaPartitionMap();
        result2.putAll(entry1.getValue());
      }
    } finally {
      schemaPartitionCacheLock.writeLock().unlock();
    }
  }

  /**
   * invalid schemaPartitionCache by storage group
   *
   * @param storageGroup the storage groups that need to invalid
   */
  public void invalidSchemaPartitionCache(String storageGroup) {
    schemaPartitionCacheLock.writeLock().lock();
    try {
      schemaPartitionCache.invalidate(storageGroup);
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
   * @param storageGroupToQueryParamsMap storage group to dataPartitionQueryParam map
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
      // check cache for each storage group
      for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
          storageGroupToQueryParamsMap.entrySet()) {
        if (!getStorageGroupDataPartition(dataPartitionMap, entry.getKey(), entry.getValue())) {
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
   * get dataPartition from storage group
   *
   * @param dataPartitionMap result
   * @param storageGroup storage group that need to get
   * @param dataPartitionQueryParams specific query params of data partition
   * @return whether hit
   */
  private boolean getStorageGroupDataPartition(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap,
      String storageGroup,
      List<DataPartitionQueryParam> dataPartitionQueryParams) {
    DataPartitionTable dataPartitionTable = dataPartitionCache.getIfPresent(storageGroup);
    if (null == dataPartitionTable) {
      logger.debug(
          "[{} Cache] miss when search storage group {}", DATA_PARTITION_CACHE_NAME, storageGroup);
      return false;
    }
    Map<TSeriesPartitionSlot, SeriesPartitionTable> cachedStorageGroupPartitionMap =
        dataPartitionTable.getDataPartitionMap();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
        seriesSlotToTimePartitionMap =
            dataPartitionMap.computeIfAbsent(storageGroup, k -> new HashMap<>());
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
   * @param cachedStorageGroupPartitionMap all cached data partition map of related storage group
   * @return whether hit
   */
  private boolean getDeviceDataPartition(
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          seriesSlotToTimePartitionMap,
      DataPartitionQueryParam dataPartitionQueryParam,
      Map<TSeriesPartitionSlot, SeriesPartitionTable> cachedStorageGroupPartitionMap) {
    TSeriesPartitionSlot seriesPartitionSlot =
        partitionExecutor.getSeriesPartitionSlot(dataPartitionQueryParam.getDevicePath());
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
   * get dataPartition from timeSlot
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
    if (null == cacheConsensusGroupId || 0 == cacheConsensusGroupId.size()) {
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
   * @param dataPartitionTable storage group to seriesPartitionSlot to timePartitionSlot to
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
        String storageGroup = entry1.getKey();
        DataPartitionTable result = dataPartitionCache.getIfPresent(storageGroup);
        if (null == result) {
          result = new DataPartitionTable();
          dataPartitionCache.put(storageGroup, result);
        }
        Map<TSeriesPartitionSlot, SeriesPartitionTable> result2 = result.getDataPartitionMap();
        for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
            entry2 : entry1.getValue().entrySet()) {
          TSeriesPartitionSlot seriesPartitionSlot = entry2.getKey();
          SeriesPartitionTable seriesPartitionTable;
          if (!result2.containsKey(seriesPartitionSlot)) {
            // if device not exists, then add new seriesPartitionTable
            seriesPartitionTable = new SeriesPartitionTable(entry2.getValue());
            result2.put(seriesPartitionSlot, seriesPartitionTable);
          } else {
            // if device exists, then merge
            seriesPartitionTable = result2.get(seriesPartitionSlot);
            Map<TTimePartitionSlot, List<TConsensusGroupId>> result3 =
                seriesPartitionTable.getSeriesPartitionMap();
            result3.putAll(entry2.getValue());
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
   * @param storageGroup the storage groups that need to invalid
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
