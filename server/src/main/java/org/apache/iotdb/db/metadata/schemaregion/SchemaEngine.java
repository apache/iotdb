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

package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.metric.SchemaMetricManager;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.db.metadata.rescon.ISchemaEngineStatistics;
import org.apache.iotdb.db.metadata.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.metadata.rescon.SchemaResourceManager;
import org.apache.iotdb.external.api.ISeriesNumerMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
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

  private volatile Map<SchemaRegionId, ISchemaRegion> schemaRegionMap;

  private ScheduledExecutorService timedForceMLogThread;

  // seriesNumberMonitor may be null
  private ISeriesNumerMonitor seriesNumerMonitor = null;

  private ISchemaEngineStatistics schemaEngineStatistics;

  private static class SchemaEngineManagerHolder {

    private static final SchemaEngine INSTANCE = new SchemaEngine();

    private SchemaEngineManagerHolder() {}
  }

  private SchemaEngine() {

    schemaRegionLoader = new SchemaRegionLoader();

    // init ISeriesNumerMonitor if there is.
    // each mmanager instance will generate an ISeriesNumerMonitor instance
    // So, if you want to share the ISeriesNumerMonitor instance, pls change this part of code.
    ServiceLoader<ISeriesNumerMonitor> monitorServiceLoader =
        ServiceLoader.load(ISeriesNumerMonitor.class);
    for (ISeriesNumerMonitor loader : monitorServiceLoader) {
      if (this.seriesNumerMonitor != null) {
        // it means there is more than one ISeriesNumerMonitor implementation.
        logger.warn("There are more than one ISeriesNumerMonitor implementation. pls check.");
      }
      logger.info("Will set seriesNumerMonitor from {} ", loader.getClass().getName());
      this.seriesNumerMonitor = loader;
    }
  }

  public static SchemaEngine getInstance() {
    return SchemaEngineManagerHolder.INSTANCE;
  }

  public void init() {
    logger.info("used schema engine mode: {}.", config.getSchemaEngineMode());

    schemaRegionLoader.init(config.getSchemaEngineMode());

    initSchemaEngineStatistics();
    SchemaResourceManager.initSchemaResource(schemaEngineStatistics);
    // CachedSchemaEngineMetric depend on CacheMemoryManager, so it should be initialized after
    // CacheMemoryManager
    SchemaMetricManager.getInstance().init(schemaEngineStatistics);

    schemaRegionMap = new ConcurrentHashMap<>();

    initSchemaRegion();

    if (!(config.isClusterMode()
            && config
                .getSchemaRegionConsensusProtocolClass()
                .equals(ConsensusFactory.RATIS_CONSENSUS))
        && config.getSyncMlogPeriodInMs() != 0) {
      timedForceMLogThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              "SchemaEngine-TimedForceMLog-Thread");
      ScheduledExecutorUtil.unsafelyScheduleAtFixedRate(
          timedForceMLogThread,
          this::forceMlog,
          config.getSyncMlogPeriodInMs(),
          config.getSyncMlogPeriodInMs(),
          TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Scan the database and schema region directories to recover schema regions and return the
   * collected local schema partition info for localSchemaPartitionTable recovery.
   */
  private void initSchemaRegion() {
    File schemaDir = new File(config.getSchemaDir());
    File[] sgDirList = schemaDir.listFiles();

    if (sgDirList == null) {
      return;
    }

    // recover SchemaRegion concurrently
    ExecutorService schemaRegionRecoverPools =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(), "SchemaRegion-recover-task");
    List<Future<ISchemaRegion>> futures = new ArrayList<>();

    for (File file : sgDirList) {
      if (!file.isDirectory()) {
        continue;
      }

      PartialPath storageGroup;
      try {
        storageGroup = new PartialPath(file.getName());
      } catch (IllegalPathException illegalPathException) {
        // not a legal sg dir
        continue;
      }

      File sgDir = new File(config.getSchemaDir(), storageGroup.getFullPath());

      if (!sgDir.exists()) {
        continue;
      }

      File[] schemaRegionDirs = sgDir.listFiles();
      if (schemaRegionDirs == null) {
        continue;
      }

      for (File schemaRegionDir : schemaRegionDirs) {
        SchemaRegionId schemaRegionId;
        try {
          schemaRegionId = new SchemaRegionId(Integer.parseInt(schemaRegionDir.getName()));
        } catch (NumberFormatException e) {
          // the dir/file is not schemaRegionDir, ignore this.
          continue;
        }
        futures.add(
            schemaRegionRecoverPools.submit(recoverSchemaRegionTask(storageGroup, schemaRegionId)));
      }
    }

    for (Future<ISchemaRegion> future : futures) {
      try {
        ISchemaRegion schemaRegion = future.get();
        schemaRegionMap.put(schemaRegion.getSchemaRegionId(), schemaRegion);
      } catch (ExecutionException | InterruptedException | RuntimeException e) {
        logger.error("Something wrong happened during SchemaRegion recovery: {}", e.getMessage());
        e.printStackTrace();
      }
    }
    schemaRegionRecoverPools.shutdown();
  }

  private void initSchemaEngineStatistics() {
    if (IoTDBDescriptor.getInstance().getConfig().getSchemaEngineMode().equals("Memory")) {
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

    // clearSchemaResource will shut down release and flush task in Schema_File mode, which must be
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
    SchemaMetricManager.getInstance().clear();
  }

  public ISchemaRegion getSchemaRegion(SchemaRegionId regionId) {
    return schemaRegionMap.get(regionId);
  }

  public Collection<ISchemaRegion> getAllSchemaRegions() {
    return schemaRegionMap.values();
  }

  public synchronized void createSchemaRegion(
      PartialPath storageGroup, SchemaRegionId schemaRegionId) throws MetadataException {
    ISchemaRegion schemaRegion = schemaRegionMap.get(schemaRegionId);
    if (schemaRegion != null) {
      if (schemaRegion.getStorageGroupFullPath().equals(storageGroup.getFullPath())) {
        return;
      } else {
        throw new MetadataException(
            String.format(
                "SchemaRegion [%s] is duplicated between [%s] and [%s], "
                    + "and the former one has been recovered.",
                schemaRegionId,
                schemaRegion.getStorageGroupFullPath(),
                storageGroup.getFullPath()));
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
        new SchemaRegionParams(
            database, schemaRegionId, schemaEngineStatistics, seriesNumerMonitor);
    ISchemaRegion schemaRegion = schemaRegionLoader.createSchemaRegion(schemaRegionParams);
    SchemaMetricManager.getInstance().createSchemaRegionMetric(schemaRegion);
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
    SchemaMetricManager.getInstance().deleteSchemaRegionMetric(schemaRegionId.getId());
    schemaRegionMap.remove(schemaRegionId);

    // check whether the sg dir is empty
    File sgDir = new File(config.getSchemaDir(), schemaRegion.getStorageGroupFullPath());
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
        FileUtils.deleteDirectory(sgDir);
      }
    }
  }

  public void setSeriesNumerMonitor(ISeriesNumerMonitor seriesNumerMonitor) {
    this.seriesNumerMonitor = seriesNumerMonitor;
  }

  public int getSchemaRegionNumber() {
    return schemaRegionMap == null ? 0 : schemaRegionMap.size();
  }

  @TestOnly
  public ISchemaEngineStatistics getSchemaEngineStatistics() {
    return schemaEngineStatistics;
  }
}
