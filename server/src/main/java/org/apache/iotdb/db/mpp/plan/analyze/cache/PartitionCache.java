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
  private static final String DATABASE_CACHE_NAME = "Database";
  private static final String SCHEMA_PARTITION_CACHE_NAME = "SchemaPartition";
  private static final String DATA_PARTITION_CACHE_NAME = "DataPartition";

  private final String seriesSlotExecutorName = config.getSeriesPartitionExecutorClass();
  private final int seriesPartitionSlotNum = config.getSeriesPartitionSlotNum();
  private final SeriesPartitionExecutor partitionExecutor;

  /** the size of database cache/schema partition cache/data partition cache. */
  private final int cacheSize = config.getPartitionCacheSize();
  /** the cache of database. */
  private final Set<String> databaseCache = Collections.synchronizedSet(new HashSet<>());
  /** the cache of schema partition table, from database to schema partition table. */
  private final Cache<String, SchemaPartitionTable> schemaPartitionCache;
  /** the cache of data partition table, from database to data partition table. */
  private final Cache<String, DataPartitionTable> dataPartitionCache;
  /** the cache of region replica set, from consensus group id to region replica set. */
  private final Map<TConsensusGroupId, TRegionReplicaSet> groupIdToReplicaSetMap = new HashMap<>();
  /** the latest time when groupIdToReplicaSetMap updated. */
  private final AtomicLong latestUpdateTime = new AtomicLong(0);

  /** The lock of cache. */
  private final ReentrantReadWriteLock databaseCacheLock = new ReentrantReadWriteLock();

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
   * get database to device map.
   *
   * @param devicePaths the target devices
   * @param fetchDatabases whether try to get all database from config node
   * @param autoCreateDatabases whether auto create database when cache miss
   */
  public Map<String, List<String>> getDatabaseToDevice(
      List<String> devicePaths, boolean fetchDatabases, boolean autoCreateDatabases) {
    DatabaseCacheResult<List<String>> result =
        new DatabaseCacheResult<List<String>>() {
          @Override
          public void put(String device, String databaseName) {
            resultMap.computeIfAbsent(databaseName, k -> new ArrayList<>());
            resultMap.get(databaseName).add(device);
          }
        };
    getDatabaseCacheResult(result, devicePaths, fetchDatabases, autoCreateDatabases);
    return result.getResultMap();
  }

  /**
   * get device to database map.
   *
   * @param devicePaths the devices that need to hit
   * @param tryToFetch whether try to get all database from config node
   * @param isAutoCreate whether auto create database when cache miss
   */
  public Map<String, String> getDeviceToDatabase(
      List<String> devicePaths, boolean tryToFetch, boolean isAutoCreate) {
    DatabaseCacheResult<String> result =
        new DatabaseCacheResult<String>() {
          @Override
          public void put(String device, String databaseName) {
            resultMap.put(device, databaseName);
          }
        };
    getDatabaseCacheResult(result, devicePaths, tryToFetch, isAutoCreate);
    return result.getResultMap();
  }

  /**
   * get database of device.
   *
   * @param devicePath the path of device
   * @return database name, return null if cache miss
   */
  private String getDatabaseName(String devicePath) {
    synchronized (databaseCache) {
      for (String databaseName : databaseCache) {
        if (PathUtils.isStartWith(devicePath, databaseName)) {
          return databaseName;
        }
      }
    }
    return null;
  }

  /**
   * get database map from database cache.
   *
   * @param result contains hit result, missed devices and result map
   * @param devicePaths the target devices
   * @param failFast If true, return when failed. If false, return when all devices check
   */
  private void getDatabaseMap(
      DatabaseCacheResult<?> result, List<String> devicePaths, boolean failFast) {
    try {
      databaseCacheLock.readLock().lock();
      result.reset();
      for (String devicePath : devicePaths) {
        String databaseName = getDatabaseName(devicePath);
        if (null == databaseName) {
          logger.debug("[{} Cache] miss when search device {}", DATABASE_CACHE_NAME, devicePath);
          if (failFast) {
            result.setFailed();
            break;
          } else {
            result.addMissedDevice(devicePath);
          }
        } else {
          result.put(devicePath, databaseName);
        }
      }
      logger.debug("[{} Cache] hit when search device {}", DATABASE_CACHE_NAME, devicePaths);
      CacheMetricsRecorder.record(result.isSuccess(), DATABASE_CACHE_NAME);
    } finally {
      databaseCacheLock.readLock().unlock();
    }
  }

  /**
   * get map from device to database in five trys.
   *
   * @param result contains hit result, missed devices and result map
   * @param devicePaths the target devices
   * @param fetchDatabases whether try to get all database from config node
   * @param autoCreateDatabases whether auto create database when cache miss
   * @throws StatementAnalyzeException failed when borrow clients failed, metadata failed or rpc
   *     failed
   */
  private void getDatabaseCacheResult(
      DatabaseCacheResult<?> result,
      List<String> devicePaths,
      boolean fetchDatabases,
      boolean autoCreateDatabases) {
    // miss when devicePath contains *
    for (String devicePath : devicePaths) {
      if (devicePath.contains("*")) {
        return;
      }
    }
    // firstly, we try to hit devices from cache in fail fast way
    getDatabaseMap(result, devicePaths, true);
    if (!result.isSuccess() && fetchDatabases) {
      // if failed in first try and enable fetch databases from config node
      try {
        try {
          databaseCacheLock.writeLock().lock();
          // secondly, we try to hit devices from cache in fail fast way in write lock
          getDatabaseMap(result, devicePaths, true);
          if (result.isSuccess()) {
            // if success, then return
            return;
          }
          // if failed, then try to fetch all databases from config node
          try (ConfigNodeClient client =
              configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
            TDatabaseSchemaResp databaseSchemaResp = client.getMatchedDatabaseSchemas(ROOT_PATH);
            TSStatus tsStatus = databaseSchemaResp.getStatus();
            if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              Set<String> databaseNames = databaseSchemaResp.getDatabaseSchemaMap().keySet();
              databaseCache.clear();
              databaseCache.addAll(databaseNames);
            } else {
              logger.error("Failed to fetch databases from config node");
              throw new RuntimeException(new IoTDBException(tsStatus.message, tsStatus.code));
            }
          }
        } finally {
          databaseCacheLock.writeLock().unlock();
        }
        // thirdly, we try to hit devices from cache in fail fast way after fetching
        getDatabaseMap(result, devicePaths, true);
        if (result.isSuccess()) {
          // if success, then return
          return;
        }
        if (autoCreateDatabases) {
          // if failed and enable auto create database
          try {
            databaseCacheLock.writeLock().lock();
            // fourthly, we try to hit database with all missed devices in write lock
            getDatabaseMap(result, devicePaths, false);
            if (result.isSuccess()) {
              // if success, then return
              return;
            }
            try (ConfigNodeClient client =
                configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
              // try to get database needed to be created from missed device
              Set<String> databaseNamesNeedCreated = new HashSet<>();
              for (String devicePath : result.getMissedDevices()) {
                PartialPath databaseNameNeedCreated =
                    MetaUtils.getStorageGroupPathByLevel(
                        new PartialPath(devicePath), config.getDefaultStorageGroupLevel());
                databaseNamesNeedCreated.add(databaseNameNeedCreated.getFullPath());
              }
              // try to create databases one by one until done or one database fail
              Set<String> successFullyCreatedDatabase = new HashSet<>();
              for (String databaseName : databaseNamesNeedCreated) {
                TDatabaseSchema databaseSchema = new TDatabaseSchema();
                databaseSchema.setName(databaseName);
                TSStatus tsStatus = client.setDatabase(databaseSchema);
                if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == tsStatus.getCode()) {
                  successFullyCreatedDatabase.add(databaseName);
                } else {
                  // try to update cache by databases successfully created
                  databaseCache.addAll(successFullyCreatedDatabase);
                  logger.warn(
                      "[{} Cache] failed to create database {}", DATABASE_CACHE_NAME, databaseName);
                  throw new RuntimeException(new IoTDBException(tsStatus.message, tsStatus.code));
                }
              }
              // try to update database cache when all databases have already been created
              databaseCache.addAll(databaseNamesNeedCreated);
            }
          } finally {
            databaseCacheLock.writeLock().unlock();
          }
        }
        // fifthly, we try to hit database in fail fast way after auto create database
        getDatabaseMap(result, devicePaths, true);
        if (!result.isSuccess()) {
          throw new StatementAnalyzeException("Failed to get database Map in three attempts.");
        }
      } catch (TException | MetadataException | ClientManagerException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getDeviceToDatabase():" + e.getMessage());
      }
    }
  }

  /**
   * update database cache.
   *
   * @param databaseNames the database names that need to update
   */
  public void updateDatabaseCache(Set<String> databaseNames) {
    databaseCacheLock.writeLock().lock();
    try {
      databaseCache.addAll(databaseNames);
    } finally {
      databaseCacheLock.writeLock().unlock();
    }
  }

  /**
   * invalidate database cache.
   *
   * @param databaseNames the databases that need to invalid
   */
  public void removeFromDatabaseCache(List<String> databaseNames) {
    databaseCacheLock.writeLock().lock();
    try {
      databaseNames.forEach(databaseCache::remove);
    } finally {
      databaseCacheLock.writeLock().unlock();
    }
  }

  /** invalidate all database cache. */
  public void invalidDatabaseCache() {
    databaseCacheLock.writeLock().lock();
    try {
      databaseCache.clear();
    } finally {
      databaseCacheLock.writeLock().unlock();
    }
  }

  // endregion

  // region replicaSet cache
  /**
   * get regionReplicaSet from local and config node.
   *
   * @param consensusGroupId the id of consensus group
   * @return regionReplicaSet
   * @throws RuntimeException if failed to get regionReplicaSet from config node
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
      // if cache miss then try to get regionReplicaSet from config node
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
            // if config node don't have then will throw RuntimeException
            if (!groupIdToReplicaSetMap.containsKey(consensusGroupId)) {
              // failed to get RegionReplicaSet from config node
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
    return result;
  }

  /**
   * update regionReplicaSetMap according to timestamp.
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

  /** invalidate replicaSetCache. */
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
   * get schemaPartition.
   *
   * @param databaseToDeviceMap database to devices map
   * @return SchemaPartition of databaseToDeviceMap
   */
  public SchemaPartition getSchemaPartition(Map<String, List<String>> databaseToDeviceMap) {
    schemaPartitionCacheLock.readLock().lock();
    try {
      if (databaseToDeviceMap.size() == 0) {
        CacheMetricsRecorder.record(false, SCHEMA_PARTITION_CACHE_NAME);
        return null;
      }
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
          new HashMap<>();

      // check cache for each database
      for (Map.Entry<String, List<String>> entry : databaseToDeviceMap.entrySet()) {
        String databaseName = entry.getKey();
        Map<TSeriesPartitionSlot, TRegionReplicaSet> regionReplicaSetMap =
            schemaPartitionMap.computeIfAbsent(databaseName, k -> new HashMap<>());
        SchemaPartitionTable schemaPartitionTable = schemaPartitionCache.getIfPresent(databaseName);
        if (null == schemaPartitionTable) {
          // if database miss, then return cache miss.
          logger.debug(
              "[{} Cache] miss when search database {}", SCHEMA_PARTITION_CACHE_NAME, databaseName);
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
        String databaseName = entry1.getKey();
        SchemaPartitionTable result = schemaPartitionCache.getIfPresent(databaseName);
        if (null == result) {
          result = new SchemaPartitionTable();
          schemaPartitionCache.put(databaseName, result);
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
   * invalid schemaPartitionCache by database.
   *
   * @param databaseName the databases that need to invalid
   */
  public void invalidSchemaPartitionCache(String databaseName) {
    schemaPartitionCacheLock.writeLock().lock();
    try {
      schemaPartitionCache.invalidate(databaseName);
    } finally {
      schemaPartitionCacheLock.writeLock().unlock();
    }
  }

  /** invalid all schemaPartitionCache. */
  public void invalidSchemaPartitionCache() {
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
   * get dataPartition by query param map.
   *
   * @param databaseToQueryParamsMap database to dataPartitionQueryParam map
   * @return DataPartition of databaseToQueryParamsMap
   */
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> databaseToQueryParamsMap) {
    dataPartitionCacheLock.readLock().lock();
    try {
      if (databaseToQueryParamsMap.size() == 0) {
        CacheMetricsRecorder.record(false, DATA_PARTITION_CACHE_NAME);
        return null;
      }
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap = new HashMap<>();
      // check cache for each database
      for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
          databaseToQueryParamsMap.entrySet()) {
        if (null == entry.getValue()
            || entry.getValue().isEmpty()
            || !getDatabaseDataPartition(dataPartitionMap, entry.getKey(), entry.getValue())) {
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
   * get dataPartition from database.
   *
   * @param dataPartitionMap result
   * @param databaseName database that need to get
   * @param dataPartitionQueryParams specific query params of data partition
   * @return whether hit
   */
  private boolean getDatabaseDataPartition(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap,
      String databaseName,
      List<DataPartitionQueryParam> dataPartitionQueryParams) {
    DataPartitionTable dataPartitionTable = dataPartitionCache.getIfPresent(databaseName);
    if (null == dataPartitionTable) {
      logger.debug(
          "[{} Cache] miss when search database {}", DATA_PARTITION_CACHE_NAME, databaseName);
      return false;
    }
    Map<TSeriesPartitionSlot, SeriesPartitionTable> cachedDatabasePartitionMap =
        dataPartitionTable.getDataPartitionMap();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
        seriesSlotToTimePartitionMap =
            dataPartitionMap.computeIfAbsent(databaseName, k -> new HashMap<>());
    // check cache for each device
    for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
      if (!getDeviceDataPartition(
          seriesSlotToTimePartitionMap, dataPartitionQueryParam, cachedDatabasePartitionMap)) {
        return false;
      }
    }
    return true;
  }

  /**
   * get dataPartition from device.
   *
   * @param seriesSlotToTimePartitionMap result
   * @param dataPartitionQueryParam specific query param of data partition
   * @param cachedDatabasePartitionMap all cached data partition map of related database
   * @return whether hit
   */
  private boolean getDeviceDataPartition(
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          seriesSlotToTimePartitionMap,
      DataPartitionQueryParam dataPartitionQueryParam,
      Map<TSeriesPartitionSlot, SeriesPartitionTable> cachedDatabasePartitionMap) {
    TSeriesPartitionSlot seriesPartitionSlot;
    if (null != dataPartitionQueryParam.getDevicePath()) {
      seriesPartitionSlot =
          partitionExecutor.getSeriesPartitionSlot(dataPartitionQueryParam.getDevicePath());
    } else {
      return false;
    }
    SeriesPartitionTable cachedSeriesPartitionTable =
        cachedDatabasePartitionMap.get(seriesPartitionSlot);
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
   * get dataPartition from time slot.
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
   * update dataPartitionCache by dataPartition.
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
        String databaseName = entry1.getKey();
        if (null != databaseName) {
          DataPartitionTable result = dataPartitionCache.getIfPresent(databaseName);
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
            dataPartitionCache.put(databaseName, result);
          }
        }
      }
    } finally {
      dataPartitionCacheLock.writeLock().unlock();
    }
  }

  /**
   * invalid dataPartitionCache by database.
   *
   * @param database the databases that need to invalid
   */
  public void invalidDataPartitionCache(String database) {
    dataPartitionCacheLock.writeLock().lock();
    try {
      dataPartitionCache.invalidate(database);
    } finally {
      dataPartitionCacheLock.writeLock().unlock();
    }
  }

  /** invalid all dataPartitionCache. */
  public void invalidDataPartitionCache() {
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
    invalidDatabaseCache();
    invalidDataPartitionCache();
    invalidSchemaPartitionCache();
    invalidReplicaSetCache();
    logger.debug("[Partition Cache] is invalid:{}", this);
  }

  @Override
  public String toString() {
    return "PartitionCache{"
        + "cacheSize="
        + cacheSize
        + ", databaseCache="
        + databaseCache
        + ", replicaSetCache="
        + groupIdToReplicaSetMap
        + ", schemaPartitionCache="
        + schemaPartitionCache
        + ", dataPartitionCache="
        + dataPartitionCache
        + '}';
  }
}
