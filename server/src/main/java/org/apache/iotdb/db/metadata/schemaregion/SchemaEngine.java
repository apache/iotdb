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

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.MTreeLoadTaskManager;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaConfLoader;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaRegion;
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
import java.util.concurrent.ConcurrentHashMap;

// manage all the schemaRegion in this dataNode
public class SchemaEngine {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final IStorageGroupSchemaManager localStorageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();

  private Map<SchemaRegionId, ISchemaRegion> schemaRegionMap;
  private SchemaEngineMode schemaRegionStoredMode;
  private RSchemaConfLoader rSchemaConfLoader;
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

    // to load MTree concurrently
    MTreeLoadTaskManager loadTaskManager = MTreeLoadTaskManager.getInstance();
    loadTaskManager.init();

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
        // createSchemaRegion(storageGroup, schemaRegionId);
        loadTaskManager.submit(createSchemaRegionTask(storageGroup, schemaRegionId));
        schemaRegionIdList.add(schemaRegionId);
      }
    }

    loadTaskManager.clear();
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
    long debugDur = System.currentTimeMillis();
    ISchemaRegion schemaRegion = schemaRegionMap.get(schemaRegionId);
    if (schemaRegion != null) {
      return;
    }
    localStorageGroupSchemaManager.ensureStorageGroup(storageGroup);
    IStorageGroupMNode storageGroupMNode =
        localStorageGroupSchemaManager.getStorageGroupNodeByStorageGroupPath(storageGroup);
    switch (schemaRegionStoredMode) {
      case Memory:
        schemaRegion = new SchemaRegionMemoryImpl(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      case Schema_File:
        schemaRegion =
            new SchemaRegionSchemaFileImpl(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      case Rocksdb_based:
        schemaRegion =
            new RSchemaRegion(
                storageGroup, schemaRegionId, storageGroupMNode, loadRocksdbConfFile());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "This mode [%s] is not supported. Please check and modify it.",
                schemaRegionStoredMode));
    }
    schemaRegionMap.put(schemaRegionId, schemaRegion);
    debugDur = System.currentTimeMillis() - debugDur;
    logger.info(
        String.format(
            "Recover [%s] spend: %s ms",
            storageGroup.concatNode(schemaRegionId.toString()), debugDur));
  }

  private Runnable createSchemaRegionTask(PartialPath storageGroup, SchemaRegionId schemaRegionId) {
    // this method is called for concurrent recovery of schema regions
    return () -> {
      long debugDur = System.currentTimeMillis();
      ISchemaRegion schemaRegion = this.schemaRegionMap.get(schemaRegionId);
      if (schemaRegion != null) {
        return;
      }
      try {
        this.localStorageGroupSchemaManager.ensureStorageGroup(storageGroup);
        IStorageGroupMNode storageGroupMNode =
            this.localStorageGroupSchemaManager.getStorageGroupNodeByStorageGroupPath(storageGroup);
        switch (this.schemaRegionStoredMode) {
          case Memory:
            schemaRegion =
                new SchemaRegionMemoryImpl(storageGroup, schemaRegionId, storageGroupMNode);
            break;
          case Schema_File:
            schemaRegion =
                new SchemaRegionSchemaFileImpl(storageGroup, schemaRegionId, storageGroupMNode);
            break;
          case Rocksdb_based:
            schemaRegion =
                new RSchemaRegion(
                    storageGroup, schemaRegionId, storageGroupMNode, loadRocksdbConfFile());
            break;
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "This mode [%s] is not supported. Please check and modify it.",
                    schemaRegionStoredMode));
        }
        this.schemaRegionMap.put(schemaRegionId, schemaRegion);
        debugDur = System.currentTimeMillis() - debugDur;
        logger.info(
            String.format(
                "Recover [%s] spend: %s ms",
                storageGroup.concatNode(schemaRegionId.toString()), debugDur));
      } catch (MetadataException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public void deleteSchemaRegion(SchemaRegionId schemaRegionId) throws MetadataException {
    schemaRegionMap.get(schemaRegionId).deleteSchemaRegion();
    schemaRegionMap.remove(schemaRegionId);
  }

  private RSchemaConfLoader loadRocksdbConfFile() {
    if (rSchemaConfLoader == null) {
      rSchemaConfLoader = new RSchemaConfLoader();
    }
    return rSchemaConfLoader;
  }
}
