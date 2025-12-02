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
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PartitionCache {

  private static final Logger logger = LoggerFactory.getLogger(PartitionCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final DataNodeMemoryConfig memoryConfig =
      IoTDBDescriptor.getInstance().getMemoryConfig();
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
            .exactAllocate(DataNodeMemoryConfig.PARTITION_CACHE, MemoryBlockType.STATIC);
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
    databaseCacheLock.readLock().lock();
    try {
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
                .setIsTableModel(false)
                .setCanSeeAuditDB(true);
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
              .setIsTableModel(true)
              .setCanSeeAuditDB(true);
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
          } else if (PathUtils.isStartWith(deviceID, SchemaConstant.AUDIT_DATABASE)) {
            databaseNamesNeedCreated.add(SchemaConstant.AUDIT_DATABASE);
          } else {
            final PartialPath databaseNameNeedCreated =
                MetaUtils.getDatabasePathByLevel(
                    new PartialPath(deviceID), config.getDefaultDatabaseLevel());
            databaseNamesNeedCreated.add(databaseNameNeedCreated.getFullPath());
          }
        }

        // Try to create databases one by one until done or one database fail
        final Set<String> successFullyCreatedDatabase = new HashSet<>();
        for (final String databaseName : databaseNamesNeedCreated) {
          final long startTime = System.nanoTime();
          try {
            final TSStatus status =
                AuthorityChecker.getAccessControl()
                    .checkCanCreateDatabaseForTree(
                        AuthorityChecker.createIAuditEntity(
                            userName, SessionManager.getInstance().getCurrSession()),
                        new PartialPath(databaseName));
            if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              throw new IoTDBRuntimeException(status);
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
            throw new IoTDBRuntimeException(tsStatus.message, tsStatus.code);
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
        IClientSession clientSession = SessionManager.getInstance().getCurrSession();
        IAuditEntity entity = AuthorityChecker.createIAuditEntity(userName, clientSession);
        AuthorityChecker.getAccessControl().checkCanCreateDatabase(userName, database, entity);
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
        throw new IoTDBRuntimeException(tsStatus.message, tsStatus.code);
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
    databaseCacheLock.readLock().lock();
    try {
      // reset result before try
      result.reset();
      boolean status = true;
      for (final IDeviceID devicePath : deviceIDs) {
        final String databaseName = getDatabaseName(devicePath);
        if (null == databaseName) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "[{} Cache] miss when search device {}",
                CacheMetrics.DATABASE_CACHE_NAME,
                devicePath);
          }
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
      if (logger.isDebugEnabled()) {
        logger.debug(
            "[{} Cache] hit when search device {}", CacheMetrics.DATABASE_CACHE_NAME, deviceIDs);
      }
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
        if (!result.isSuccess()) {
          if (isAutoCreate) {
            // try to auto create database of failed device
            createDatabaseAndUpdateCache(result, deviceIDs, userName);
            if (!result.isSuccess()) {
              throw new StatementAnalyzeException("Failed to get database Map");
            }
          } else {
            // check if it is to auto create the system or audit database
            for (IDeviceID deviceID : deviceIDs) {
              if (!deviceID.isTableModel()
                  && deviceID.startWith("root." + SystemConstant.SYSTEM_PREFIX_KEY)) {
                createDatabaseAndUpdateCache(result, Collections.singletonList(deviceID), userName);
                break;
              }
              if (!deviceID.isTableModel()
                  && deviceID.startWith("root." + SystemConstant.AUDIT_PREFIX_KEY)) {
                createDatabaseAndUpdateCache(result, Collections.singletonList(deviceID), userName);
                break;
              }
            }
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
   * @param consensusGroupIds the ids of consensus group
   * @return List<regionReplicaSet>
   * @throws RuntimeException if failed to get regionReplicaSet from configNode
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
      // if not hit then try to get regionReplicaSet from configNode
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
            // if configNode don't have then will throw RuntimeException
            if (result.isEmpty()) {
              // failed to get RegionReplicaSet from configNode
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
          if (logger.isDebugEnabled()) {
            logger.debug(
                "[{} Cache] miss when search database {}",
                CacheMetrics.SCHEMA_PARTITION_CACHE_NAME,
                databaseName);
          }
          cacheMetrics.record(false, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
          return null;
        }
        final Map<TSeriesPartitionSlot, TConsensusGroupId> map =
            schemaPartitionTable.getSchemaPartitionMap();
        // check cache for each device
        List<TSeriesPartitionSlot> seriesPartitionSlots = new ArrayList<>(entry.getValue().size());
        List<TConsensusGroupId> consensusGroupIds = new ArrayList<>(entry.getValue().size());
        for (final IDeviceID device : entry.getValue()) {
          final TSeriesPartitionSlot seriesPartitionSlot =
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
        if (logger.isDebugEnabled()) {
          logger.debug(
              "[{} Cache] miss when search database {}",
              CacheMetrics.SCHEMA_PARTITION_CACHE_NAME,
              database);
        }
        cacheMetrics.record(false, CacheMetrics.SCHEMA_PARTITION_CACHE_NAME);
        return null;
      }
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
          new HashMap<>();
      Map<TSeriesPartitionSlot, TRegionReplicaSet> regionReplicaSetMap =
          schemaPartitionMap.computeIfAbsent(database, k -> new HashMap<>());

      Map<TSeriesPartitionSlot, TConsensusGroupId> orderedMap =
          new LinkedHashMap<>(schemaPartitionTable.getSchemaPartitionMap());
      List<TConsensusGroupId> orderedGroupIds = new ArrayList<>(orderedMap.values());
      List<TRegionReplicaSet> regionReplicaSets = getRegionReplicaSet(orderedGroupIds);

      int index = 0;
      for (Map.Entry<TSeriesPartitionSlot, TConsensusGroupId> entry : orderedMap.entrySet()) {
        regionReplicaSetMap.put(entry.getKey(), regionReplicaSets.get(index++));
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

      final Set<TConsensusGroupId> allConsensusGroupIds = new HashSet<>();
      final Map<TConsensusGroupId, HashSet<TimeSlotRegionInfo>> consensusGroupToTimeSlotMap =
          new HashMap<>();

      for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
          databaseToQueryParamsMap.entrySet()) {
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
          if (null != param.getDeviceID()) {
            seriesPartitionSlot = partitionExecutor.getSeriesPartitionSlot(param.getDeviceID());
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
                  param.getDeviceID());
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
    if (logger.isDebugEnabled()) {
      logger.debug("[Partition Cache] invalid");
    }
    removeFromDatabaseCache();
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
