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
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.storagegroup.IStorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.storagegroup.StorageGroupSchemaManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

// manage all the schemaRegion in this dataNode
public class SchemaEngine {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final IStorageGroupSchemaManager localStorageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();

  private Map<SchemaRegionId, ISchemaRegion> schemaRegionMap;
  private SchemaEngineMode schemaRegionStoredMode;
  private static final Logger logger = LoggerFactory.getLogger(SchemaEngine.class);

  private static class SchemaEngineManagerHolder {

    private static final SchemaEngine INSTANCE = new SchemaEngine();

    private SchemaEngineManagerHolder() {}
  }

  private SchemaEngine() {}

  public static SchemaEngine getInstance() {
    return SchemaEngineManagerHolder.INSTANCE;
  }

  public Map<PartialPath, List<SchemaRegionId>> init() throws MetadataException {
    schemaRegionMap = new ConcurrentHashMap<>();
    schemaRegionStoredMode = SchemaEngineMode.valueOf(config.getSchemaEngineMode());
    logger.info("used schema engine mode: {}.", schemaRegionStoredMode);

    return initSchemaRegion();
  }

  /**
   * Scan the storage group and schema region directories to recover schema regions and return the
   * collected local schema partition info for localSchemaPartitionTable recovery.
   */
  private Map<PartialPath, List<SchemaRegionId>> initSchemaRegion() throws MetadataException {
    Map<PartialPath, List<SchemaRegionId>> partitionTable = new HashMap<>();

    // recover SchemaRegion concurrently
    ExecutorService schemaRegionRecoverPools =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(), "SchemaRegion-recover-task");
    List<Future<ISchemaRegion>> futures = new ArrayList<>();

    for (PartialPath storageGroup : localStorageGroupSchemaManager.getAllStorageGroupPaths()) {
      List<SchemaRegionId> schemaRegionIdList = new ArrayList<>();
      partitionTable.put(storageGroup, schemaRegionIdList);

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
        schemaRegionIdList.add(schemaRegionId);
      }
    }

    for (Future<ISchemaRegion> future : futures) {
      try {
        ISchemaRegion schemaRegion = future.get();
        schemaRegionMap.put(schemaRegion.getSchemaRegionId(), schemaRegion);
      } catch (ExecutionException | InterruptedException | RuntimeException e) {
        logger.error("Something wrong happened during SchemaRegion recovery: " + e.getMessage());
        e.printStackTrace();
      }
    }
    schemaRegionRecoverPools.shutdown();

    return partitionTable;
  }

  public void forceMlog() {
    if (schemaRegionMap != null) {
      for (ISchemaRegion schemaRegion : schemaRegionMap.values()) {
        schemaRegion.forceMlog();
      }
    }
  }

  public void clear() {
    if (schemaRegionMap != null) {
      for (ISchemaRegion schemaRegion : schemaRegionMap.values()) {
        schemaRegion.clear();
      }
      schemaRegionMap.clear();
      schemaRegionMap = null;
    }
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
        // TODO: handle duplicated regionId across different storage group
        ISchemaRegion schemaRegion =
            createSchemaRegionWithoutExistenceCheck(storageGroup, schemaRegionId);
        timeRecord = System.currentTimeMillis() - timeRecord;
        logger.info(
            String.format(
                "Recover [%s] spend: %s ms",
                storageGroup.concatNode(schemaRegionId.toString()), timeRecord));
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
      PartialPath storageGroup, SchemaRegionId schemaRegionId) throws MetadataException {
    ISchemaRegion schemaRegion = null;
    this.localStorageGroupSchemaManager.ensureStorageGroup(storageGroup);
    IStorageGroupMNode storageGroupMNode =
        this.localStorageGroupSchemaManager.getStorageGroupNodeByStorageGroupPath(storageGroup);
    switch (this.schemaRegionStoredMode) {
      case Memory:
        schemaRegion = new SchemaRegionMemoryImpl(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      case Schema_File:
        schemaRegion =
            new SchemaRegionSchemaFileImpl(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      case Rocksdb_based:
        schemaRegion =
            new RSchemaRegionLoader()
                .loadRSchemaRegion(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "This mode [%s] is not supported. Please check and modify it.",
                schemaRegionStoredMode));
    }
    return schemaRegion;
  }

  public void deleteSchemaRegion(SchemaRegionId schemaRegionId) throws MetadataException {
    schemaRegionMap.get(schemaRegionId).deleteSchemaRegion();
    schemaRegionMap.remove(schemaRegionId);
  }
}
