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
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.storagegroup.IStorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.storagegroup.StorageGroupSchemaManager;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// manage all the schemaRegion in this dataNode
public class SchemaEngine {

  private final IStorageGroupSchemaManager localStorageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();

  private Map<SchemaRegionId, SchemaRegion> schemaRegionMap;

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
  }

  public void clear() {
    if (schemaRegionMap != null) {
      schemaRegionMap.clear();
      schemaRegionMap = null;
    }
  }

  public SchemaRegion getSchemaRegion(SchemaRegionId regionId) {
    return schemaRegionMap.get(regionId);
  }

  public Collection<SchemaRegion> getAllSchemaRegions() {
    return schemaRegionMap.values();
  }

  public void createSchemaRegion(String storageGroup, SchemaRegionId schemaRegionId)
      throws MetadataException {
    createSchemaRegion(new PartialPath(storageGroup), schemaRegionId);
  }

  public synchronized void createSchemaRegion(
      PartialPath storageGroup, SchemaRegionId schemaRegionId) throws MetadataException {
    SchemaRegion schemaRegion = schemaRegionMap.get(schemaRegionId);
    if (schemaRegion != null) {
      return;
    }
    PartialPath checkedStorageGroup =
        localStorageGroupSchemaManager.ensureStorageGroup(storageGroup);
    if (!checkedStorageGroup.equals(storageGroup)) {
      throw new StorageGroupAlreadySetException(checkedStorageGroup.getFullPath());
    }
    schemaRegion =
        new SchemaRegion(
            storageGroup,
            schemaRegionId,
            localStorageGroupSchemaManager.getStorageGroupNodeByStorageGroupPath(storageGroup));
    schemaRegionMap.put(schemaRegionId, schemaRegion);
  }

  public void deleteSchemaRegion(SchemaRegionId schemaRegionId) throws MetadataException {
    schemaRegionMap.remove(schemaRegionId).deleteSchemaRegion();
  }
}
