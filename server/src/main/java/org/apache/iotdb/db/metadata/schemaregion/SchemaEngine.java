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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.LocalSchemaPartitionTable;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.storagegroup.IStorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.storagegroup.StorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// manage all the schemaRegion in this dataNode
public class SchemaEngine {

  private final IStorageGroupSchemaManager localStorageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();

  private Map<SchemaRegionId, ISchemaRegion> schemaRegionMap;
  private SchemaEngineMode schemaRegionStoredMode;
  private static final Logger logger = LoggerFactory.getLogger(SchemaEngine.class);

  private LocalSchemaPartitionTable partitionTable = LocalSchemaPartitionTable.getInstance();

  private static class SchemaEngineManagerHolder {
    private static final SchemaEngine INSTANCE = new SchemaEngine();

    private SchemaEngineManagerHolder() {}
  }

  private SchemaEngine() {}

  public static SchemaEngine getInstance() {
    return SchemaEngineManagerHolder.INSTANCE;
  }

  public void init() {
    schemaRegionMap = new ConcurrentHashMap<>();
    schemaRegionStoredMode =
        SchemaEngineMode.valueOf(IoTDBDescriptor.getInstance().getConfig().getSchemaEngineMode());
    logger.info("used schema engine mode: {}.", schemaRegionStoredMode);
  }

  public void forceMlog() {
    if (schemaRegionMap != null) {
      for (SchemaRegion schemaRegion : schemaRegionMap.values()) {
        schemaRegion.forceMlog();
      }
    }
  }

  public void clear() {
    if (schemaRegionMap != null) {
      for (SchemaRegion schemaRegion : schemaRegionMap.values()) {
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
    SchemaRegion schemaRegion = schemaRegionMap.get(schemaRegionId);
    if (schemaRegion != null) {
      return;
    }
    localStorageGroupSchemaManager.ensureStorageGroup(storageGroup);
    schemaRegion =
        new SchemaRegion(
            storageGroup,
            schemaRegionId,
            localStorageGroupSchemaManager.getStorageGroupNodeByStorageGroupPath(storageGroup));

    switch (schemaRegionStoredMode) {
      case Memory:
      case Schema_File:
        schemaRegion = new SchemaRegion(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      case Rocksdb_based:
        schemaRegion = new RSchemaRegion(storageGroup, schemaRegionId, storageGroupMNode);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "This mode [%s] is not supported. Please check and modify it.",
                schemaRegionStoredMode));
    }
    schemaRegionMap.put(schemaRegionId, schemaRegion);
    partitionTable.putSchemaRegionId(storageGroup, schemaRegionId);
  }

  public void deleteSchemaRegion(PartialPath storageGroup, SchemaRegionId schemaRegionId)
      throws MetadataException {
    partitionTable.removeSchemaRegionId(storageGroup, schemaRegionId);
    schemaRegionMap.get(schemaRegionId).deleteSchemaRegion();
    schemaRegionMap.remove(schemaRegionId);
  }
}
