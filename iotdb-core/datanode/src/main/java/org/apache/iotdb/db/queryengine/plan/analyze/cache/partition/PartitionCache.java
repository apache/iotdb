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

package org.apache.iotdb.db.queryengine.plan.analyze.cache.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaUtils;
import org.apache.iotdb.db.service.metrics.CacheMetrics;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PartitionCache {

  private static final Logger logger = LoggerFactory.getLogger(PartitionCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final List<String> ROOT_PATH = Arrays.asList("root", "**");

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
  private final CacheMetrics cacheMetrics;

  public PartitionCache() {
    this.schemaPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    this.dataPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    this.partitionExecutor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            this.seriesSlotExecutorName, this.seriesPartitionSlotNum);
    this.cacheMetrics = new CacheMetrics();
  }

  // region database cache

  /**
   * get database to device map
   *
   * @param devicePaths the devices that need to hit
   * @param tryToFetch whether try to get all database from config node
   * @param isAutoCreate whether auto create database when cache miss
   * @param userName
   */
  public Map<String, List<String>> getStorageGroupToDevice(
      List<String> devicePaths, boolean tryToFetch, boolean isAutoCreate, String userName) {
    StorageGroupCacheResult<List<String>> result =
        new StorageGroupCacheResult<List<String>>() {
          @Override
          public void put(String device, String storageGroupName) {
            map.computeIfAbsent(storageGroupName, k -> new ArrayList<>());
            map.get(storageGroupName).add(device);
          }
        };
    getStorageGroupCacheResult(result, devicePaths, tryToFetch, isAutoCreate, userName);
    return result.getMap();
  }

  /**
   * get device to database map
   *
   * @param devicePaths the devices that need to hit
   * @param tryToFetch whether try to get all database from config node
   * @param isAutoCreate whether auto create database when cache miss
   * @param userName
   */
  public Map<String, String> getDeviceToStorageGroup(
      List<String> devicePaths, boolean tryToFetch, boolean isAutoCreate, String userName) {
    StorageGroupCacheResult<String> result =
        new StorageGroupCacheResult<String>() {
          @Override
          public void put(String device, String storageGroupName) {
            map.put(device, storageGroupName);
          }
        };
    getStorageGroupCacheResult(result, devicePaths, tryToFetch, isAutoCreate, userName);
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
    storageGroupCacheLock.writeLock().lock();
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      result.reset();
      getStorageGroupMap(result, devicePaths, true);
      if (!result.isSuccess()) {
        TGetDatabaseReq req = new TGetDatabaseReq(ROOT_PATH, SchemaConstant.ALL_MATCH_SCOPE_BINARY);
        TDatabaseSchemaResp storageGroupSchemaResp = client.getMatchedDatabaseSchemas(req);
        if (storageGroupSchemaResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          Set<String> storageGroupNames = storageGroupSchemaResp.getDatabaseSchemaMap().keySet();
          // update all database into cache
          updateStorageCache(storageGroupNames);
          getStorageGroupMap(result, devicePaths, true);
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
   * @param userName
   * @throws RuntimeException if failed to create database
   */
  private void createStorageGroupAndUpdateCache(
      StorageGroupCacheResult<?> result, List<String> devicePaths, String userName)
      throws ClientManagerException, MetadataException, TException {
    storageGroupCacheLock.writeLock().lock();
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Try to check whether database need to be created
      result.reset();
      // Try to hit database with all missed devices
      getStorageGroupMap(result, devicePaths, false);
      if (!result.isSuccess()) {
        // Try to get database needed to be created from missed device
        Set<String> storageGroupNamesNeedCreated = new HashSet<>();
        for (String devicePath : result.getMissedDevices()) {
          if (devicePath.equals(SchemaConstant.SYSTEM_DATABASE)
              || devicePath.startsWith(SchemaConstant.SYSTEM_DATABASE + ".")) {
            storageGroupNamesNeedCreated.add(SchemaConstant.SYSTEM_DATABASE);
          } else {
            PartialPath storageGroupNameNeedCreated =
                MetaUtils.getStorageGroupPathByLevel(
                    new PartialPath(devicePath), config.getDefaultStorageGroupLevel());
            storageGroupNamesNeedCreated.add(storageGroupNameNeedCreated.getFullPath());
          }
        }

        // Try to create databases one by one until done or one database fail
        Set<String> successFullyCreatedStorageGroup = new HashSet<>();
        for (String storageGroupName : storageGroupNamesNeedCreated) {
          long startTime = System.nanoTime();
          try {
            if (!AuthorityChecker.SUPER_USER.equals(userName)) {
              TSStatus status =
                  AuthorityChecker.getTSStatus(
                      AuthorityChecker.checkSystemPermission(
                          userName, PrivilegeType.MANAGE_DATABASE.ordinal()),
                      PrivilegeType.MANAGE_DATABASE);
              if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                throw new RuntimeException(
                    new IoTDBException(status.getMessage(), status.getCode()));
              }
            }
          } finally {
            PerformanceOverviewMetrics.getInstance().recordAuthCost(System.nanoTime() - startTime);
          }
          TDatabaseSchema storageGroupSchema = new TDatabaseSchema();
          storageGroupSchema.setName(storageGroupName);
          if (SchemaConstant.SYSTEM_DATABASE.equals(storageGroupName)) {
            storageGroupSchema.setMinSchemaRegionGroupNum(1);
            storageGroupSchema.setMaxSchemaRegionGroupNum(1);
            storageGroupSchema.setMaxDataRegionGroupNum(1);
            storageGroupSchema.setMaxDataRegionGroupNum(1);
          }
          TSStatus tsStatus = client.setDatabase(storageGroupSchema);
          if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == tsStatus.getCode()) {
            successFullyCreatedStorageGroup.add(storageGroupName);
          } else {
            // Try to update cache by databases successfully created
            updateStorageCache(successFullyCreatedStorageGroup);
            logger.warn(
                "[{} Cache] failed to create database {}",
                CacheMetrics.STORAGE_GROUP_CACHE_NAME,
                storageGroupName);
            throw new RuntimeException(new IoTDBException(tsStatus.message, tsStatus.code));
          }
        }
        // Try to update database cache when all databases has already been created
        updateStorageCache(storageGroupNamesNeedCreated);
        getStorageGroupMap(result, devicePaths, false);
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
    storageGroupCacheLock.readLock().lock();
    try {
      // reset result before try
      result.reset();
      boolean status = true;
      for (String devicePath : devicePaths) {
        String storageGroupName = getStorageGroupName(devicePath);
        if (null == storageGroupName) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "[{} Cache] miss when search device {}",
                CacheMetrics.STORAGE_GROUP_CACHE_NAME,
                devicePath);
          }
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
      if (logger.isDebugEnabled()) {
        logger.debug(
            "[{} Cache] hit when search device {}",
            CacheMetrics.STORAGE_GROUP_CACHE_NAME,
            devicePaths);
      }
      cacheMetrics.record(status, CacheMetrics.STORAGE_GROUP_CACHE_NAME);
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
   * @param userName
   */
  private void getStorageGroupCacheResult(
      StorageGroupCacheResult<?> result,
      List<String> devicePaths,
      boolean tryToFetch,
      boolean isAutoCreate,
      String userName) {
    if (!isAutoCreate) {
      // miss when devicePath contains *
      for (String devicePath : devicePaths) {
        if (devicePath.contains("*")) {
          return;
        }
      }
    }
    // first try to hit database in fast-fail way
    getStorageGroupMap(result, devicePaths, true);
    if (!result.isSuccess() && tryToFetch) {
      try {
        // try to fetch database from config node when miss
        fetchStorageGroupAndUpdateCache(result, devicePaths);
        if (!result.isSuccess() && isAutoCreate) {
          // try to auto create database of failed device
          createStorageGroupAndUpdateCache(result, devicePaths, userName);
          if (!result.isSuccess()) {
            throw new StatementAnalyzeException("Failed to get database Map");
          }
        }
      } catch (MetadataException e) {
        throw new IoTDBRuntimeException(
            "An error occurred when executing getDeviceToDatabase():" + e.getMessage(),
            e.getErrorCode());
      } catch (TException | ClientManagerException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getDeviceToDatabase():" + e.getMessage(), e);
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
   * @param consensusGroupIds the ids of consensus group
   * @return List<regionReplicaSet>
   * @throws RuntimeException if failed to get regionReplicaSet from confignode
   * @throws StatementAnalyzeException if there are exception when try to get latestRegionRouteMap
   */
  public List<TRegionReplicaSet> getRegionReplicaSet(List<TConsensusGroupId> consensusGroupIds) {
    if (consensusGroupIds.isEmpty()) {
      return Collections.emptyList();
    }
    List<TRegionReplicaSet> result;
    // try to get regionReplicaSet from cache
    regionReplicaSetLock.readLock().lock();
    try {
      result = getRegionReplicaSetInternal(consensusGroupIds);
    } finally {
      regionReplicaSetLock.readLock().unlock();
    }
    if (result.isEmpty()) {
      // if not hit then try to get regionReplicaSet from confignode
      regionReplicaSetLock.writeLock().lock();
      try {
        // double check after getting the write lock
        result = getRegionReplicaSetInternal(consensusGroupIds);
        if (result.isEmpty()) {
          try (ConfigNodeClient client =
              configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
            TRegionRouteMapResp resp = client.getLatestRegionRouteMap();
            if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == resp.getStatus().getCode()) {
              updateGroupIdToReplicaSetMap(resp.getTimestamp(), resp.getRegionRouteMap());
            } else {
              logger.warn(
                  "Unexpected error when getRegionReplicaSet: status {}， regionMap: {}",
                  resp.getStatus(),
                  resp.getRegionRouteMap());
            }
            result = getRegionReplicaSetInternal(consensusGroupIds);
            // if confignode don't have then will throw RuntimeException
            if (result.isEmpty()) {
              // failed to get RegionReplicaSet from confignode
              throw new RuntimeException(
                  "Failed to get replicaSet of consensus groups[ids= " + consensusGroupIds + "]");
            }
          } catch (ClientManagerException | TException e) {
            throw new StatementAnalyzeException(
                "An error occurred when executing getRegionReplicaSet():" + e.getMessage());
          }
        }
      } finally {
        regionReplicaSetLock.writeLock().unlock();
      }
    }
    // try to get regionReplicaSet by consensusGroupId
    return result;
  }

  private List<TRegionReplicaSet> getRegionReplicaSetInternal(
      List<TConsensusGroupId> consensusGroupIds) {
    List<TRegionReplicaSet> result = new ArrayList<>(consensusGroupIds.size());
    for (TConsensusGroupId groupId : consensusGroupIds) {
      TRegionReplicaSet replicaSet = groupIdToReplicaSetMap.get(groupId);
      if (replicaSet != null) {
        result.add(replicaSet);
      } else {
        return Collections.emptyList();
      }
    }
    return result;
  }

  /**
   * update regionReplicaSetMap according to timestamp
   *
   * @param timestamp the timestamp of map that need to update
   * @param map consensusGroupId to regionReplicaSet map
   * @return {@code true} if update successfully or false when map is not latest
   */
  public boolean updateGroupIdToReplicaSetMap(
      long timestamp, Map<TConsensusGroupId, TRegionReplicaSet> map) {
    regionReplicaSetLock.writeLock().lock();
    try {
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
    regionReplicaSetLock.writeLock().lock();
    try {
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
        cacheMetrics.record(false, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
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
          if (logger.isDebugEnabled()) {
            logger.debug(
                "[{} Cache] miss when search database {}",
                CacheMetrics.SCHEMA_PARTITION_CACHE_NAME,
                storageGroupName);
          }
          cacheMetrics.record(false, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
          return null;
        }
        Map<TSeriesPartitionSlot, TConsensusGroupId> map =
            schemaPartitionTable.getSchemaPartitionMap();
        // check cache for each device
        List<TSeriesPartitionSlot> seriesPartitionSlots = new ArrayList<>(entry.getValue().size());
        List<TConsensusGroupId> consensusGroupIds = new ArrayList<>(entry.getValue().size());
        for (String device : entry.getValue()) {
          TSeriesPartitionSlot seriesPartitionSlot =
              partitionExecutor.getSeriesPartitionSlot(device);
          if (!map.containsKey(seriesPartitionSlot)) {
            // if one device not find, then return cache miss.
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "[{} Cache] miss when search device {}",
                  CacheMetrics.SCHEMA_PARTITION_CACHE_NAME,
                  device);
            }
            cacheMetrics.record(false, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
            return null;
          }
          seriesPartitionSlots.add(seriesPartitionSlot);
          consensusGroupIds.add(map.get(seriesPartitionSlot));
        }
        List<TRegionReplicaSet> replicaSets = getRegionReplicaSet(consensusGroupIds);
        for (int i = 0; i < replicaSets.size(); i++) {
          regionReplicaSetMap.put(seriesPartitionSlots.get(i), replicaSets.get(i));
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("[{} Cache] hit", CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
      }
      // cache hit
      cacheMetrics.record(true, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
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
      if (storageGroupToQueryParamsMap.isEmpty()) {
        cacheMetrics.record(false, CacheMetrics.DATA_PARTITION_CACHE_NAME);
        return null;
      }

      final Set<TConsensusGroupId> allConsensusGroupIds = new HashSet<>();
      final Map<TConsensusGroupId, HashSet<TimeSlotRegionInfo>> consensusGroupToTimeSlotMap =
          new HashMap<>();

      // check cache for each database
      for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
          storageGroupToQueryParamsMap.entrySet()) {
        String databaseName = entry.getKey();
        List<DataPartitionQueryParam> params = entry.getValue();

        if (null == params || params.isEmpty()) {
          cacheMetrics.record(false, CacheMetrics.DATA_PARTITION_CACHE_NAME);
          return null;
        }

        DataPartitionTable dataPartitionTable = dataPartitionCache.getIfPresent(databaseName);
        if (null == dataPartitionTable) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "[{} Cache] miss when search database {}",
                CacheMetrics.DATA_PARTITION_CACHE_NAME,
                databaseName);
          }
          cacheMetrics.record(false, CacheMetrics.DATA_PARTITION_CACHE_NAME);
          return null;
        }

        Map<TSeriesPartitionSlot, SeriesPartitionTable> cachedDatabasePartitionMap =
            dataPartitionTable.getDataPartitionMap();

        for (DataPartitionQueryParam param : params) {
          TSeriesPartitionSlot seriesPartitionSlot;
          if (null != param.getDevicePath()) {
            seriesPartitionSlot = partitionExecutor.getSeriesPartitionSlot(param.getDevicePath());
          } else {
            return null;
          }

          SeriesPartitionTable cachedSeriesPartitionTable =
              cachedDatabasePartitionMap.get(seriesPartitionSlot);
          if (null == cachedSeriesPartitionTable) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "[{} Cache] miss when search device {}",
                  CacheMetrics.DATA_PARTITION_CACHE_NAME,
                  param.getDevicePath());
            }
            cacheMetrics.record(false, CacheMetrics.DATA_PARTITION_CACHE_NAME);
            return null;
          }

          Map<TTimePartitionSlot, List<TConsensusGroupId>> cachedTimePartitionSlot =
              cachedSeriesPartitionTable.getSeriesPartitionMap();

          if (param.getTimePartitionSlotList().isEmpty()) {
            return null;
          }

          for (TTimePartitionSlot timePartitionSlot : param.getTimePartitionSlotList()) {
            List<TConsensusGroupId> cacheConsensusGroupIds =
                cachedTimePartitionSlot.get(timePartitionSlot);
            if (null == cacheConsensusGroupIds
                || cacheConsensusGroupIds.isEmpty()
                || null == timePartitionSlot) {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "[{} Cache] miss when search time partition {}",
                    CacheMetrics.DATA_PARTITION_CACHE_NAME,
                    timePartitionSlot);
              }
              cacheMetrics.record(false, CacheMetrics.DATA_PARTITION_CACHE_NAME);
              return null;
            }

            for (TConsensusGroupId groupId : cacheConsensusGroupIds) {
              allConsensusGroupIds.add(groupId);
              consensusGroupToTimeSlotMap
                  .computeIfAbsent(groupId, k -> new HashSet<>())
                  .add(
                      new TimeSlotRegionInfo(databaseName, seriesPartitionSlot, timePartitionSlot));
            }
          }
        }
      }

      final List<TConsensusGroupId> consensusGroupIds = new ArrayList<>(allConsensusGroupIds);
      final List<TRegionReplicaSet> allRegionReplicaSets = getRegionReplicaSet(consensusGroupIds);

      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap = new HashMap<>();

      for (int i = 0; i < allRegionReplicaSets.size(); i++) {
        TConsensusGroupId groupId = consensusGroupIds.get(i);
        TRegionReplicaSet replicaSet = allRegionReplicaSets.get(i);

        for (TimeSlotRegionInfo info : consensusGroupToTimeSlotMap.get(groupId)) {
          dataPartitionMap
              .computeIfAbsent(info.databaseName, k -> new HashMap<>())
              .computeIfAbsent(info.seriesPartitionSlot, k -> new HashMap<>())
              .computeIfAbsent(info.timePartitionSlot, k -> new ArrayList<>())
              .add(replicaSet);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("[{} Cache] hit", CacheMetrics.DATA_PARTITION_CACHE_NAME);
      }

      cacheMetrics.record(true, CacheMetrics.DATA_PARTITION_CACHE_NAME);
      return new DataPartition(dataPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
    } finally {
      dataPartitionCacheLock.readLock().unlock();
    }
  }

  private static class TimeSlotRegionInfo {
    final String databaseName;
    final TSeriesPartitionSlot seriesPartitionSlot;
    final TTimePartitionSlot timePartitionSlot;

    TimeSlotRegionInfo(
        String databaseName,
        TSeriesPartitionSlot seriesPartitionSlot,
        TTimePartitionSlot timePartitionSlot) {
      this.databaseName = databaseName;
      this.seriesPartitionSlot = seriesPartitionSlot;
      this.timePartitionSlot = timePartitionSlot;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TimeSlotRegionInfo that = (TimeSlotRegionInfo) o;
      return Objects.equals(databaseName, that.databaseName)
          && Objects.equals(seriesPartitionSlot, that.seriesPartitionSlot)
          && Objects.equals(timePartitionSlot, that.timePartitionSlot);
    }

    @Override
    public int hashCode() {
      int result = Objects.hashCode(databaseName);
      result = 31 * result + Objects.hashCode(seriesPartitionSlot);
      result = 31 * result + Objects.hashCode(timePartitionSlot);
      return result;
    }
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
    if (logger.isDebugEnabled()) {
      logger.debug("[Partition Cache] invalid");
    }
    removeFromStorageGroupCache();
    invalidAllDataPartitionCache();
    invalidAllSchemaPartitionCache();
    invalidReplicaSetCache();
    if (logger.isDebugEnabled()) {
      logger.debug("[Partition Cache] is invalid:{}", this);
    }
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
