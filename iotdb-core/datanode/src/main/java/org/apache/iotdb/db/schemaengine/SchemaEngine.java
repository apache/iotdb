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

package org.apache.iotdb.db.schemaengine;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.schemaengine.metric.ISchemaRegionMetric;
import org.apache.iotdb.db.schemaengine.metric.SchemaMetricManager;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.DataNodeSchemaQuotaManager;
import org.apache.iotdb.db.schemaengine.rescon.ISchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.SchemaResourceManager;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionParams;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionLoader;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionParams;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// manage all the schemaRegion in this dataNode
public class SchemaEngine {

  private static final Logger logger = LoggerFactory.getLogger(SchemaEngine.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final SchemaRegionLoader schemaRegionLoader;

  @SuppressWarnings("java:S3077")
  private volatile Map<SchemaRegionId, ISchemaRegion> schemaRegionMap;

  private ScheduledExecutorService timedForceMLogThread;

  private ISchemaEngineStatistics schemaEngineStatistics;

  private SchemaMetricManager schemaMetricManager;

  private final DataNodeSchemaQuotaManager schemaQuotaManager =
      DataNodeSchemaQuotaManager.getInstance();

  private static class SchemaEngineManagerHolder {

    private static final SchemaEngine INSTANCE = new SchemaEngine();

    private SchemaEngineManagerHolder() {}
  }

  private SchemaEngine() {
    schemaRegionLoader = new SchemaRegionLoader();
  }

  public static SchemaEngine getInstance() {
    return SchemaEngineManagerHolder.INSTANCE;
  }

  public void init() {
    logger.info(
        "used schema engine mode: {}.",
        CommonDescriptor.getInstance().getConfig().getSchemaEngineMode());

    schemaRegionLoader.init(CommonDescriptor.getInstance().getConfig().getSchemaEngineMode());

    initSchemaEngineStatistics();
    SchemaResourceManager.initSchemaResource(schemaEngineStatistics);
    // CachedSchemaEngineMetric depend on CacheMemoryManager, so it should be initialized after
    // CacheMemoryManager
    schemaMetricManager = new SchemaMetricManager(schemaEngineStatistics);

    schemaRegionMap = new ConcurrentHashMap<>();

    initSchemaRegion();

    if (!(config.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS))
        && config.getSyncMlogPeriodInMs() != 0) {
      timedForceMLogThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.SCHEMA_FORCE_MLOG.getName());
      ScheduledExecutorUtil.unsafelyScheduleAtFixedRate(
          timedForceMLogThread,
          this::forceMlog,
          config.getSyncMlogPeriodInMs(),
          config.getSyncMlogPeriodInMs(),
          TimeUnit.MILLISECONDS);
    }
  }

  public static Map<String, List<SchemaRegionId>> getLocalSchemaRegionInfo() {
    final File schemaDir = new File(config.getSchemaDir());
    final File[] sgDirList = schemaDir.listFiles();
    final Map<String, List<SchemaRegionId>> localSchemaPartitionTable = new HashMap<>();
    if (sgDirList == null) {
      return localSchemaPartitionTable;
    }
    for (File file : sgDirList) {
      if (!file.isDirectory()) {
        continue;
      }

      final PartialPath database;
      try {
        database = PartialPath.getDatabasePath(file.getName());
      } catch (IllegalPathException illegalPathException) {
        // not a legal sg dir
        continue;
      }

      final File sgDir = new File(config.getSchemaDir(), database.getFullPath());

      if (!sgDir.exists()) {
        continue;
      }

      final File[] schemaRegionDirs = sgDir.listFiles();
      if (schemaRegionDirs == null) {
        continue;
      }
      List<SchemaRegionId> schemaRegionIds = new ArrayList<>();
      for (final File schemaRegionDir : schemaRegionDirs) {
        final SchemaRegionId schemaRegionId;
        try {
          schemaRegionId = new SchemaRegionId(Integer.parseInt(schemaRegionDir.getName()));
        } catch (final NumberFormatException e) {
          // the dir/file is not schemaRegionDir, ignore this.
          continue;
        }
        schemaRegionIds.add(schemaRegionId);
      }
      localSchemaPartitionTable.put(database.getFullPath(), schemaRegionIds);
    }
    return localSchemaPartitionTable;
  }

  /**
   * Scan the database and schema region directories to recover schema regions and return the
   * collected local schema partition info for localSchemaPartitionTable recovery.
   */
  @SuppressWarnings("java:S2142")
  private void initSchemaRegion() {
    // recover SchemaRegion concurrently
    Map<String, List<SchemaRegionId>> localSchemaRegionInfo = getLocalSchemaRegionInfo();
    final ExecutorService schemaRegionRecoverPools =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            ThreadName.SCHEMA_REGION_RECOVER_TASK.getName());
    final List<Future<ISchemaRegion>> futures = new ArrayList<>();
    localSchemaRegionInfo.forEach(
        (k, v) -> {
          for (SchemaRegionId schemaRegionId : v) {
            PartialPath database;
            try {
              database = PartialPath.getDatabasePath(k);
            } catch (IllegalPathException e) {
              logger.warn("Illegal database path: {}", k);
              continue;
            }
            futures.add(
                schemaRegionRecoverPools.submit(recoverSchemaRegionTask(database, schemaRegionId)));
          }
        });
    for (final Future<ISchemaRegion> future : futures) {
      try {
        final ISchemaRegion schemaRegion = future.get();
        schemaRegionMap.put(schemaRegion.getSchemaRegionId(), schemaRegion);
      } catch (final ExecutionException | InterruptedException | RuntimeException e) {
        logger.error("Something wrong happened during SchemaRegion recovery", e);
      }
    }
    schemaRegionRecoverPools.shutdown();
  }

  private void initSchemaEngineStatistics() {
    if (CommonDescriptor.getInstance().getConfig().getSchemaEngineMode().equals("Memory")) {
      schemaEngineStatistics = new MemSchemaEngineStatistics();
    } else {
      schemaEngineStatistics = new CachedSchemaEngineStatistics();
    }
  }

  public void forceMlog() {
    Map<SchemaRegionId, ISchemaRegion> schemaRegionMap = this.schemaRegionMap;
    if (schemaRegionMap != null) {
      for (ISchemaRegion schemaRegion : schemaRegionMap.values()) {
        schemaRegion.forceMlog();
      }
    }
  }

  public void clear() {
    schemaRegionLoader.clear();

    // clearSchemaResource will shut down release and flush task in PBTree mode, which must be
    // down before clear schema region
    SchemaResourceManager.clearSchemaResource();
    if (timedForceMLogThread != null) {
      timedForceMLogThread.shutdown();
      timedForceMLogThread = null;
    }

    if (schemaRegionMap != null) {
      // SchemaEngineStatistics will be clear after clear all schema region
      for (ISchemaRegion schemaRegion : schemaRegionMap.values()) {
        schemaRegion.clear();
      }
      schemaRegionMap.clear();
      schemaRegionMap = null;
    }
    // SchemaMetric should be cleared lastly
    if (schemaMetricManager != null) {
      schemaMetricManager.clear();
    }
    ClusterTemplateManager.getInstance().clear();
  }

  public ISchemaRegion getSchemaRegion(SchemaRegionId regionId) {
    return schemaRegionMap.get(regionId);
  }

  public Collection<ISchemaRegion> getAllSchemaRegions() {
    return schemaRegionMap.values();
  }

  public List<SchemaRegionId> getAllSchemaRegionIds() {
    return new ArrayList<>(schemaRegionMap.keySet());
  }

  public synchronized void createSchemaRegion(
      PartialPath storageGroup, SchemaRegionId schemaRegionId) throws MetadataException {
    ISchemaRegion schemaRegion = schemaRegionMap.get(schemaRegionId);
    if (schemaRegion != null) {
      if (schemaRegion.getDatabaseFullPath().equals(storageGroup.getFullPath())) {
        return;
      } else {
        throw new MetadataException(
            String.format(
                "SchemaRegion [%s] is duplicated between [%s] and [%s], "
                    + "and the former one has been recovered.",
                schemaRegionId, schemaRegion.getDatabaseFullPath(), storageGroup.getFullPath()));
      }
    }
    schemaRegionMap.put(
        schemaRegionId, createSchemaRegionWithoutExistenceCheck(storageGroup, schemaRegionId));
  }

  private Callable<ISchemaRegion> recoverSchemaRegionTask(
      PartialPath storageGroup, SchemaRegionId schemaRegionId) {
    // this method is called for concurrent recovery of schema regions
    return () -> {
      long timeRecord = System.currentTimeMillis();
      try {
        // TODO: handle duplicated regionId across different database
        ISchemaRegion schemaRegion =
            createSchemaRegionWithoutExistenceCheck(storageGroup, schemaRegionId);
        timeRecord = System.currentTimeMillis() - timeRecord;
        logger.info(
            "Recover [{}] spend: {} ms",
            storageGroup.concatNode(schemaRegionId.toString()),
            timeRecord);
        return schemaRegion;
      } catch (MetadataException e) {
        logger.error(
            String.format(
                "SchemaRegion [%d] in StorageGroup [%s] failed to recover.",
                schemaRegionId.getId(), storageGroup.getFullPath()));
        throw new RuntimeException(e);
      }
    };
  }

  private ISchemaRegion createSchemaRegionWithoutExistenceCheck(
      PartialPath database, SchemaRegionId schemaRegionId) throws MetadataException {
    ISchemaRegionParams schemaRegionParams =
        new SchemaRegionParams(database, schemaRegionId, schemaEngineStatistics);
    ISchemaRegion schemaRegion = schemaRegionLoader.createSchemaRegion(schemaRegionParams);
    schemaMetricManager.addSchemaRegionMetric(
        schemaRegionId.getId(), schemaRegion.getSchemaRegionMetric());
    return schemaRegion;
  }

  public synchronized void deleteSchemaRegion(SchemaRegionId schemaRegionId)
      throws MetadataException {
    ISchemaRegion schemaRegion = schemaRegionMap.get(schemaRegionId);
    if (schemaRegion == null) {
      logger.warn("SchemaRegion(id = {}) has been deleted, skiped", schemaRegionId);
      return;
    }
    schemaRegion.deleteSchemaRegion();
    schemaMetricManager.removeSchemaRegionMetric(schemaRegionId.getId());
    schemaRegionMap.remove(schemaRegionId);

    // check whether the sg dir is empty
    File sgDir = new File(config.getSchemaDir(), schemaRegion.getDatabaseFullPath());
    File[] regionDirList =
        sgDir.listFiles(
            (dir, name) -> {
              try {
                Integer.parseInt(name);
                return true;
              } catch (NumberFormatException e) {
                return false;
              }
            });
    // remove the empty sg dir
    if (regionDirList == null || regionDirList.length == 0) {
      if (sgDir.exists()) {
        FileUtils.deleteFileOrDirectory(sgDir);
      }
    }
  }

  public int getSchemaRegionNumber() {
    return schemaRegionMap == null ? 0 : schemaRegionMap.size();
  }

  public Map<Integer, Long> countDeviceNumBySchemaRegion(final List<Integer> schemaIds) {
    final Map<Integer, Long> deviceNum = new HashMap<>();

    schemaRegionMap.entrySet().stream()
        .filter(
            entry ->
                schemaIds.contains(entry.getKey().getId())
                    && SchemaRegionConsensusImpl.getInstance().isLeader(entry.getKey()))
        .forEach(
            entry ->
                deviceNum.put(
                    entry.getKey().getId(),
                    entry.getValue().getSchemaRegionStatistics().getDevicesNumber()));
    return deviceNum;
  }

  public Map<Integer, Long> countTimeSeriesNumBySchemaRegion(final List<Integer> schemaIds) {
    final Map<Integer, Long> timeSeriesNum = new HashMap<>();
    schemaRegionMap.entrySet().stream()
        .filter(
            entry ->
                schemaIds.contains(entry.getKey().getId())
                    && SchemaRegionConsensusImpl.getInstance().isLeader(entry.getKey()))
        .forEach(
            entry ->
                timeSeriesNum.put(entry.getKey().getId(), getTimeSeriesNumber(entry.getValue())));
    return timeSeriesNum;
  }

  // not including view number
  private long getTimeSeriesNumber(ISchemaRegion schemaRegion) {
    return schemaRegion.getSchemaRegionStatistics().getSeriesNumber(false)
        + schemaRegion.getSchemaRegionStatistics().getTable2DevicesNumMap().entrySet().stream()
            .map(
                tableEntry -> {
                  final TsTable table =
                      DataNodeTableCache.getInstance()
                          .getTable(
                              PathUtils.unQualifyDatabaseName(schemaRegion.getDatabaseFullPath()),
                              tableEntry.getKey());
                  return Objects.nonNull(table)
                      ? table.getMeasurementNum() * tableEntry.getValue()
                      : 0;
                })
            .reduce(0L, Long::sum);
  }

  /**
   * Update total count in schema quota manager and generate local count map response. If limit is
   * not -1 and deviceNumMap/timeSeriesNumMap is null, fill deviceNumMap/timeSeriesNumMap of the
   * SchemaRegion whose current node is the leader
   *
   * @param req heartbeat request
   * @param resp heartbeat response
   */
  public void updateAndFillSchemaCountMap(
      final TDataNodeHeartbeatReq req, final TDataNodeHeartbeatResp resp) {
    // update DataNodeSchemaQuotaManager
    schemaQuotaManager.updateRemain(
        req.getTimeSeriesQuotaRemain(),
        req.isSetDeviceQuotaRemain() ? req.getDeviceQuotaRemain() : -1);
    if (schemaQuotaManager.isDeviceLimit()) {
      if (resp.getRegionDeviceUsageMap() == null) {
        resp.setRegionDeviceUsageMap(new HashMap<>());
      }
      final Map<Integer, Long> tmp = resp.getRegionDeviceUsageMap();
      SchemaRegionConsensusImpl.getInstance().getAllConsensusGroupIds().stream()
          .filter(
              consensusGroupId ->
                  SchemaRegionConsensusImpl.getInstance().isLeader(consensusGroupId))
          .forEach(
              consensusGroupId ->
                  tmp.put(
                      consensusGroupId.getId(),
                      Optional.ofNullable(schemaRegionMap.get(consensusGroupId))
                          .map(
                              schemaRegion ->
                                  schemaRegion.getSchemaRegionStatistics().getDevicesNumber())
                          .orElse(0L)));
    }
    if (schemaQuotaManager.isMeasurementLimit()) {
      if (resp.getRegionSeriesUsageMap() == null) {
        resp.setRegionSeriesUsageMap(new HashMap<>());
      }
      final Map<Integer, Long> tmp = resp.getRegionSeriesUsageMap();
      SchemaRegionConsensusImpl.getInstance().getAllConsensusGroupIds().stream()
          .filter(
              consensusGroupId ->
                  SchemaRegionConsensusImpl.getInstance().isLeader(consensusGroupId))
          .forEach(
              consensusGroupId ->
                  tmp.put(
                      consensusGroupId.getId(),
                      Optional.ofNullable(schemaRegionMap.get(consensusGroupId))
                          .map(this::getTimeSeriesNumber)
                          .orElse(0L)));
    }
  }

  public ISchemaEngineStatistics getSchemaEngineStatistics() {
    return schemaEngineStatistics;
  }

  public ISchemaRegionMetric getSchemaRegionMetric(int schemaRegionId) {
    return schemaMetricManager.getSchemaRegionMetric(schemaRegionId);
  }
}
