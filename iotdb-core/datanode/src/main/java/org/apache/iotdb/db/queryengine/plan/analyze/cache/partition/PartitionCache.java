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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.memory.MemoryConfig;
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
import org.apache.tsfile.file.metadata.IDeviceID;
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
  private static final MemoryConfig memoryConfig = MemoryConfig.getInstance();
  private static final List<String> ROOT_PATH = Arrays.asList("root", "**");

  /** calculate slotId by device */
  private final String seriesSlotExecutorName = config.getSeriesPartitionExecutorClass();

  private final int seriesPartitionSlotNum = config.getSeriesPartitionSlotNum();
  private final SeriesPartitionExecutor partitionExecutor;

  /** the cache of database */
  private final Set<String> databaseCache = new HashSet<>();

  /** database -> schemaPartitionTable */
  private final Cache<String, SchemaPartitionTable> schemaPartitionCache;

  /** database -> dataPartitionTable */
  private final Cache<String, DataPartitionTable> dataPartitionCache;

  /** the latest time when groupIdToReplicaSetMap updated. */
  private final AtomicLong latestUpdateTime = new AtomicLong(0);

  /** TConsensusGroupId -> TRegionReplicaSet */
  private final Map<TConsensusGroupId, TRegionReplicaSet> groupIdToReplicaSetMap = new HashMap<>();

  /** The lock of cache */
  private final ReentrantReadWriteLock databaseCacheLock = new ReentrantReadWriteLock();

  private final ReentrantReadWriteLock schemaPartitionCacheLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock dataPartitionCacheLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock regionReplicaSetLock = new ReentrantReadWriteLock();

  private final IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager =
      ConfigNodeClientManager.getInstance();

  private final CacheMetrics cacheMetrics;
  private final IMemoryBlock memoryBlock;

  public PartitionCache() {
    this.memoryBlock =
        memoryConfig
            .getPartitionCacheMemoryManager()
            .forceAllocate("PartitionCache", MemoryBlockType.FUNCTION);
    this.memoryBlock.allocate(this.memoryBlock.getTotalMemorySizeInBytes());
    // TODO @spricoder: PartitionCache need to be controlled according to memory
    this.schemaPartitionCache =
        Caffeine.newBuilder().maximumSize(config.getPartitionCacheSize()).build();
    this.dataPartitionCache =
        Caffeine.newBuilder().maximumSize(config.getPartitionCacheSize()).build();
    this.partitionExecutor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            this.seriesSlotExecutorName, this.seriesPartitionSlotNum);
    this.cacheMetrics = new CacheMetrics();
  }

  // region database cache

  /**
   * get database to device map
   *
   * @param deviceIDs the devices that need to hit
   * @param tryToFetch whether try to get all database from config node
   * @param isAutoCreate whether auto create database when cache miss
   * @param userName the userName
   */
  public Map<String, List<IDeviceID>> getDatabaseToDevice(
      final List<IDeviceID> deviceIDs,
      final boolean tryToFetch,
      final boolean isAutoCreate,
      final String userName) {
    final DatabaseCacheResult<String, List<IDeviceID>> result =
        new DatabaseCacheResult<String, List<IDeviceID>>() {
          @Override
          public void put(final IDeviceID device, final String databaseName) {
            map.computeIfAbsent(databaseName, k -> new ArrayList<>()).add(device);
          }
        };
    getDatabaseCacheResult(result, deviceIDs, tryToFetch, isAutoCreate, userName);
    return result.getMap();
  }

  /**
   * get device to database map
   *
   * @param deviceIDs the devices that need to hit
   * @param tryToFetch whether try to get all database from config node
   * @param isAutoCreate whether auto create database when cache miss
   * @param userName the userName
   */
  public Map<IDeviceID, String> getDeviceToDatabase(
      final List<IDeviceID> deviceIDs,
      final boolean tryToFetch,
      final boolean isAutoCreate,
      final String userName) {
    final DatabaseCacheResult<IDeviceID, String> result =
        new DatabaseCacheResult<IDeviceID, String>() {
          @Override
          public void put(final IDeviceID device, final String databaseName) {
            map.put(device, databaseName);
          }
        };
    getDatabaseCacheResult(result, deviceIDs, tryToFetch, isAutoCreate, userName);
    return result.getMap();
  }

  /**
   * get database of device
   *
   * @param deviceID the path of device
   * @return database name, return {@code null} if cache miss
   */
  private String getDatabaseName(final IDeviceID deviceID) {
    for (final String database : databaseCache) {
      if (PathUtils.isStartWith(deviceID, database)) {
        return database;
      }
    }
    return null;
  }

  /**
   * judge whether this database is existed
   *
   * @param database name
   * @return {@code true} if this database exists
   */
  private boolean containsDatabase(final String database) {
    try {
      databaseCacheLock.readLock().lock();
      return databaseCache.contains(database);
    } finally {
      databaseCacheLock.readLock().unlock();
    }
  }

  /**
   * get all database from configNode and update database cache
   *
   * @param result the result of get database cache
   * @param deviceIDs the devices that need to hit
   */
  private void fetchDatabaseAndUpdateCache(
      final DatabaseCacheResult<?, ?> result, final List<IDeviceID> deviceIDs)
      throws ClientManagerException, TException {
    databaseCacheLock.writeLock().lock();
    try (final ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      result.reset();
      getDatabaseMap(result, deviceIDs, true);
      if (!result.isSuccess()) {
        final TGetDatabaseReq req =
            new TGetDatabaseReq(ROOT_PATH, SchemaConstant.ALL_MATCH_SCOPE_BINARY)
                .setIsTableModel(false);
        final TDatabaseSchemaResp databaseSchemaResp = client.getMatchedDatabaseSchemas(req);
        if (databaseSchemaResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          // update all database into cache
          updateDatabaseCache(databaseSchemaResp.getDatabaseSchemaMap().keySet());
          getDatabaseMap(result, deviceIDs, true);
        }
      }
    } finally {
      databaseCacheLock.writeLock().unlock();
    }
  }

  /** get all database from configNode and update database cache. */
  private void fetchDatabaseAndUpdateCache() throws ClientManagerException, TException {
    databaseCacheLock.writeLock().lock();
    try (final ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TGetDatabaseReq req =
          new TGetDatabaseReq(ROOT_PATH, SchemaConstant.ALL_MATCH_SCOPE_BINARY)
              .setIsTableModel(true);
      final TDatabaseSchemaResp databaseSchemaResp = client.getMatchedDatabaseSchemas(req);
      if (databaseSchemaResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // update all database into cache
        updateDatabaseCache(databaseSchemaResp.getDatabaseSchemaMap().keySet());
      }
    } finally {
      databaseCacheLock.writeLock().unlock();
    }
  }

  /**
   * create not existed database and update database cache
   *
   * @param result the result of get database cache
   * @param deviceIDs the devices that need to hit
   * @param userName the username
   * @throws RuntimeException if failed to create database
   */
  private void createDatabaseAndUpdateCache(
      final DatabaseCacheResult<?, ?> result,
      final List<IDeviceID> deviceIDs,
      final String userName)
      throws ClientManagerException, MetadataException, TException {
    databaseCacheLock.writeLock().lock();
    try (final ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Try to check whether database need to be created
      result.reset();
      // Try to hit database with all missed devices
      getDatabaseMap(result, deviceIDs, false);
      if (!result.isSuccess()) {
        // Try to get database needed to be created from missed device
        final Set<String> databaseNamesNeedCreated = new HashSet<>();
        for (final IDeviceID deviceID : result.getMissedDevices()) {
          if (PathUtils.isStartWith(deviceID, SchemaConstant.SYSTEM_DATABASE)) {
            databaseNamesNeedCreated.add(SchemaConstant.SYSTEM_DATABASE);
          } else {
            final PartialPath databaseNameNeedCreated =
                MetaUtils.getDatabasePathByLevel(
                    new PartialPath(deviceID), config.getDefaultStorageGroupLevel());
            databaseNamesNeedCreated.add(databaseNameNeedCreated.getFullPath());
          }
        }

        // Try to create databases one by one until done or one database fail
        final Set<String> successFullyCreatedDatabase = new HashSet<>();
        for (final String databaseName : databaseNamesNeedCreated) {
          final long startTime = System.nanoTime();
          try {
            if (!AuthorityChecker.SUPER_USER.equals(userName)) {
              final TSStatus status =
                  AuthorityChecker.getTSStatus(
                      AuthorityChecker.checkSystemPermission(
                          userName, PrivilegeType.MANAGE_DATABASE),
                      PrivilegeType.MANAGE_DATABASE);
              if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                throw new RuntimeException(
                    new IoTDBException(status.getMessage(), status.getCode()));
              }
            }
          } finally {
            PerformanceOverviewMetrics.getInstance().recordAuthCost(System.nanoTime() - startTime);
          }
          final TDatabaseSchema databaseSchema = new TDatabaseSchema();
          databaseSchema.setName(databaseName);
          databaseSchema.setIsTableModel(false);
          final TSStatus tsStatus = client.setDatabase(databaseSchema);
          if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == tsStatus.getCode()
              || TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode() == tsStatus.getCode()) {
            successFullyCreatedDatabase.add(databaseName);
            // In tree model, if the user creates a conflict database concurrently, for instance,
            // the database created by user is root.db.ss.a, the auto-creation failed database is
            // root.db, we wait till "getOrCreatePartition" to judge if the time series (like
            // root.db.ss.a.e / root.db.ss.a) conflicts with the created database. just do not throw
            // exception here.
          } else if (TSStatusCode.DATABASE_CONFLICT.getStatusCode() != tsStatus.getCode()) {
            // Try to update cache by databases successfully created
            updateDatabaseCache(successFullyCreatedDatabase);
            logger.warn(
                "[{} Cache] failed to create database {}",
                CacheMetrics.DATABASE_CACHE_NAME,
                databaseName);
            throw new RuntimeException(new IoTDBException(tsStatus.message, tsStatus.code));
          }
        }
        // Try to update database cache when all databases have already been created
        updateDatabaseCache(successFullyCreatedDatabase);
        getDatabaseMap(result, deviceIDs, false);
      }
    } finally {
      databaseCacheLock.writeLock().unlock();
    }
  }

  /**
   * create not existed database and update database cache
   *
   * @param database the database
   * @param userName the username
   * @throws RuntimeException if failed to create database
   */
  private void createDatabaseAndUpdateCache(final String database, final String userName)
      throws ClientManagerException, TException {
    databaseCacheLock.writeLock().lock();
    try (final ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      long startTime = System.nanoTime();
      try {
        if (!AuthorityChecker.SUPER_USER.equals(userName)) {
          final TSStatus status =
              AuthorityChecker.getTSStatus(
                  AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MANAGE_DATABASE),
                  PrivilegeType.MANAGE_DATABASE);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new RuntimeException(new IoTDBException(status.getMessage(), status.getCode()));
          }
        }
      } finally {
        PerformanceOverviewMetrics.getInstance().recordAuthCost(System.nanoTime() - startTime);
      }
      final TDatabaseSchema databaseSchema = new TDatabaseSchema();
      databaseSchema.setName(database);
      databaseSchema.setIsTableModel(true);
      final TSStatus tsStatus = client.setDatabase(databaseSchema);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == tsStatus.getCode()
          || TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode() == tsStatus.getCode()) {
        // Try to update cache by databases successfully created
        updateDatabaseCache(Collections.singleton(database));
      } else {
        logger.warn(
            "[{} Cache] failed to create database {}", CacheMetrics.DATABASE_CACHE_NAME, database);
        throw new RuntimeException(new IoTDBException(tsStatus.message, tsStatus.code));
      }
    } finally {
      databaseCacheLock.writeLock().unlock();
    }
  }

  /**
   * get database map in one try
   *
   * @param result contains result(boolean), failed devices and the map
   * @param deviceIDs the devices that need to hit
   * @param failFast if {@code true}, return when failed. if {@code false}, return when all devices
   *     hit
   */
  private void getDatabaseMap(
      final DatabaseCacheResult<?, ?> result,
      final List<IDeviceID> deviceIDs,
      final boolean failFast) {
    try {
      databaseCacheLock.readLock().lock();
      // reset result before try
      result.reset();
      boolean status = true;
      for (final IDeviceID devicePath : deviceIDs) {
        final String databaseName = getDatabaseName(devicePath);
        if (null == databaseName) {
          logger.debug(
              "[{} Cache] miss when search device {}",
              CacheMetrics.DATABASE_CACHE_NAME,
              devicePath);
          status = false;
          if (failFast) {
            break;
          } else {
            result.addMissedDevice(devicePath);
          }
        } else {
          result.put(devicePath, databaseName);
        }
      }
      // setFailed the result when miss
      if (!status) {
        result.setFailed();
      }
      logger.debug(
          "[{} Cache] hit when search device {}", CacheMetrics.DATABASE_CACHE_NAME, deviceIDs);
      cacheMetrics.record(status, CacheMetrics.DATABASE_CACHE_NAME);
    } finally {
      databaseCacheLock.readLock().unlock();
    }
  }

  /**
   * get database map in three try
   *
   * @param result contains result, failed devices and map
   * @param deviceIDs the devices that need to hit
   * @param tryToFetch whether try to get all database from confignode
   * @param isAutoCreate whether auto create database when device miss
   * @param userName
   */
  private void getDatabaseCacheResult(
      final DatabaseCacheResult<?, ?> result,
      final List<IDeviceID> deviceIDs,
      final boolean tryToFetch,
      final boolean isAutoCreate,
      final String userName) {
    if (!isAutoCreate) {
      // TODO: avoid IDeviceID contains "*"
      // miss when deviceId contains *
      for (final IDeviceID deviceID : deviceIDs) {
        for (int i = 0; i < deviceID.segmentNum(); i++) {
          if (((String) deviceID.segment(i)).contains("*")) {
            return;
          }
        }
      }
    }
    // first try to hit database in fast-fail way
    getDatabaseMap(result, deviceIDs, true);
    if (!result.isSuccess() && tryToFetch) {
      try {
        // try to fetch database from config node when miss
        fetchDatabaseAndUpdateCache(result, deviceIDs);
        if (!result.isSuccess() && isAutoCreate) {
          // try to auto create database of failed device
          createDatabaseAndUpdateCache(result, deviceIDs, userName);
          if (!result.isSuccess()) {
            throw new StatementAnalyzeException("Failed to get database Map");
          }
        }
      } catch (final TException | MetadataException | ClientManagerException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getDeviceToDatabase():" + e.getMessage(), e);
      }
    }
  }

  public void checkAndAutoCreateDatabase(
      final String database, final boolean isAutoCreate, final String userName) {
    boolean isExisted = containsDatabase(database);
    if (!isExisted) {
      try {
        // try to fetch database from config node when miss
        fetchDatabaseAndUpdateCache();
        isExisted = containsDatabase(database);
        if (!isExisted && isAutoCreate) {
          // try to auto create database of failed device
          createDatabaseAndUpdateCache(database, userName);
        }
      } catch (final TException | ClientManagerException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getDeviceToDatabase():" + e.getMessage());
      }
    }
  }

  /**
   * update database cache
   *
   * @param databaseNames the database names that need to update
   */
  public void updateDatabaseCache(final Set<String> databaseNames) {
    databaseCacheLock.writeLock().lock();
    try {
      databaseCache.addAll(databaseNames);
    } finally {
      databaseCacheLock.writeLock().unlock();
    }
  }

  /** invalidate all database cache */
  public void removeFromDatabaseCache() {
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
   * get regionReplicaSet from local and configNode
   *
   * @param consensusGroupId the id of consensus group
   * @return regionReplicaSet
   * @throws RuntimeException if failed to get regionReplicaSet from configNode
   * @throws StatementAnalyzeException if there are exception when try to get latestRegionRouteMap
   */
  public TRegionReplicaSet getRegionReplicaSet(TConsensusGroupId consensusGroupId) {
    TRegionReplicaSet result;
    // try to get regionReplicaSet from cache
    regionReplicaSetLock.readLock().lock();
    try {
      result = groupIdToReplicaSetMap.get(consensusGroupId);
    } finally {
      regionReplicaSetLock.readLock().unlock();
    }
    if (result == null) {
      // if not hit then try to get regionReplicaSet from configNode
      regionReplicaSetLock.writeLock().lock();
      try {
        // verify that there are not hit in cache
        if (!groupIdToReplicaSetMap.containsKey(consensusGroupId)) {
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
            // if configNode don't have then will throw RuntimeException
            if (!groupIdToReplicaSetMap.containsKey(consensusGroupId)) {
              // failed to get RegionReplicaSet from configNode
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
   * @param databaseToDeviceMap database to devices map
   * @return SchemaPartition of databaseToDeviceMap
   */
  public SchemaPartition getSchemaPartition(
      final Map<String, List<IDeviceID>> databaseToDeviceMap) {
    schemaPartitionCacheLock.readLock().lock();
    try {
      if (databaseToDeviceMap.isEmpty()) {
        cacheMetrics.record(false, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
        return null;
      }
      final Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
          new HashMap<>();
      // check cache for each database
      for (final Map.Entry<String, List<IDeviceID>> entry : databaseToDeviceMap.entrySet()) {
        final String databaseName = entry.getKey();
        final Map<TSeriesPartitionSlot, TRegionReplicaSet> regionReplicaSetMap =
            schemaPartitionMap.computeIfAbsent(databaseName, k -> new HashMap<>());
        final SchemaPartitionTable schemaPartitionTable =
            schemaPartitionCache.getIfPresent(databaseName);
        if (null == schemaPartitionTable) {
          // if database not find, then return cache miss.
          logger.debug(
              "[{} Cache] miss when search database {}",
              CacheMetrics.SCHEMA_PARTITION_CACHE_NAME,
              databaseName);
          cacheMetrics.record(false, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
          return null;
        }
        final Map<TSeriesPartitionSlot, TConsensusGroupId> map =
            schemaPartitionTable.getSchemaPartitionMap();
        // check cache for each device
        for (final IDeviceID device : entry.getValue()) {
          final TSeriesPartitionSlot seriesPartitionSlot =
              partitionExecutor.getSeriesPartitionSlot(device);
          if (!map.containsKey(seriesPartitionSlot)) {
            // if one device not find, then return cache miss.
            logger.debug(
                "[{} Cache] miss when search device {}",
                CacheMetrics.SCHEMA_PARTITION_CACHE_NAME,
                device);
            cacheMetrics.record(false, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
            return null;
          }
          final TConsensusGroupId consensusGroupId = map.get(seriesPartitionSlot);
          final TRegionReplicaSet regionReplicaSet = getRegionReplicaSet(consensusGroupId);
          regionReplicaSetMap.put(seriesPartitionSlot, regionReplicaSet);
        }
      }
      logger.debug("[{} Cache] hit", CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
      // cache hit
      cacheMetrics.record(true, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
      return new SchemaPartition(
          schemaPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
    } finally {
      schemaPartitionCacheLock.readLock().unlock();
    }
  }

  /**
   * get schemaPartition
   *
   * @param database database
   * @return SchemaPartition of databaseToDeviceMap
   */
  public SchemaPartition getSchemaPartition(String database) {
    schemaPartitionCacheLock.readLock().lock();
    try {
      SchemaPartitionTable schemaPartitionTable = schemaPartitionCache.getIfPresent(database);
      if (null == schemaPartitionTable) {
        // if database not find, then return cache miss.
        logger.debug(
            "[{} Cache] miss when search database {}",
            CacheMetrics.SCHEMA_PARTITION_CACHE_NAME,
            database);
        cacheMetrics.record(false, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
        return null;
      }
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
          new HashMap<>();
      Map<TSeriesPartitionSlot, TRegionReplicaSet> regionReplicaSetMap =
          schemaPartitionMap.computeIfAbsent(database, k -> new HashMap<>());
      for (Map.Entry<TSeriesPartitionSlot, TConsensusGroupId> entry :
          schemaPartitionTable.getSchemaPartitionMap().entrySet()) {
        regionReplicaSetMap.put(entry.getKey(), getRegionReplicaSet(entry.getValue()));
      }
      logger.debug("[{} Cache] hit", CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
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
      final Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable) {
    schemaPartitionCacheLock.writeLock().lock();
    try {
      for (final Map.Entry<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> entry1 :
          schemaPartitionTable.entrySet()) {
        final String databaseName = entry1.getKey();
        SchemaPartitionTable result = schemaPartitionCache.getIfPresent(databaseName);
        if (null == result) {
          result = new SchemaPartitionTable();
          schemaPartitionCache.put(databaseName, result);
        }
        final Map<TSeriesPartitionSlot, TConsensusGroupId> seriesPartitionSlotTConsensusGroupIdMap =
            result.getSchemaPartitionMap();
        seriesPartitionSlotTConsensusGroupIdMap.putAll(entry1.getValue());
      }
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
   * @param databaseToQueryParamsMap database to dataPartitionQueryParam map
   * @return DataPartition of databaseToQueryParamsMap
   */
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> databaseToQueryParamsMap) {
    dataPartitionCacheLock.readLock().lock();
    try {
      if (databaseToQueryParamsMap.isEmpty()) {
        cacheMetrics.record(false, CacheMetrics.DATA_PARTITION_CACHE_NAME);
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
          cacheMetrics.record(false, CacheMetrics.DATA_PARTITION_CACHE_NAME);
          return null;
        }
      }
      logger.debug("[{} Cache] hit", CacheMetrics.DATA_PARTITION_CACHE_NAME);
      // cache hit
      cacheMetrics.record(true, CacheMetrics.DATA_PARTITION_CACHE_NAME);
      return new DataPartition(dataPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
    } finally {
      dataPartitionCacheLock.readLock().unlock();
    }
  }

  /**
   * get dataPartition from database
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
          "[{} Cache] miss when search database {}",
          CacheMetrics.DATA_PARTITION_CACHE_NAME,
          databaseName);
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
   * get dataPartition from device
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
    if (null != dataPartitionQueryParam.getDeviceID()) {
      seriesPartitionSlot =
          partitionExecutor.getSeriesPartitionSlot(dataPartitionQueryParam.getDeviceID());
    } else {
      return false;
    }
    SeriesPartitionTable cachedSeriesPartitionTable =
        cachedDatabasePartitionMap.get(seriesPartitionSlot);
    if (null == cachedSeriesPartitionTable) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "[{} Cache] miss when search device {}",
            CacheMetrics.DATA_PARTITION_CACHE_NAME,
            dataPartitionQueryParam.getDeviceID());
      }
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
          CacheMetrics.DATA_PARTITION_CACHE_NAME,
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
    removeFromDatabaseCache();
    invalidAllDataPartitionCache();
    invalidAllSchemaPartitionCache();
    invalidReplicaSetCache();
    logger.debug("[Partition Cache] is invalid:{}", this);
  }

  @Override
  public String toString() {
    return "PartitionCache{"
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
