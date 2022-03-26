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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.SchemaEngine;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.storagegroup.IStorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.storagegroup.StorageGroupSchemaManager;
import org.apache.iotdb.db.metadata.template.TemplateManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// The class is only used for 0.14 code and will be removed after new cluster
public class MockSchemaPartitionTable {

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // mock the storage group management in configNode
  private IStorageGroupSchemaManager storageGroupSchemaManager =
      StorageGroupSchemaManager.getInstance();

  // mock the template management in configNode
  private TemplateManager templateManager = TemplateManager.getInstance();

  // mock the schemaRegion management in configNode, here's only one dataNode
  private SchemaEngine schemaEngine;

  private final Map<PartialPath, Set<ISchemaRegionId>> table = new ConcurrentHashMap<>();

  public void registerSchemaEngine(SchemaEngine schemaEngine) {
    this.schemaEngine = schemaEngine;
  }

  public void init() throws MetadataException {
    for (PartialPath storageGroup : storageGroupSchemaManager.getAllStorageGroupPaths()) {
      setStorageGroup(storageGroup);

      File sgDir = new File(config.getSchemaDir(), storageGroup.getFullPath());

      if (!sgDir.exists()) {
        continue;
      }

      File[] schemaRegionDirs = sgDir.listFiles();
      if (schemaRegionDirs == null) {
        continue;
      }

      for (File schemaRegionDir : schemaRegionDirs) {
        ISchemaRegionId schemaRegionId =
            ISchemaRegionId.getISchemaRegionId(sgDir.getName(), schemaRegionDir.getName());
        table.get(storageGroup).add(schemaRegionId);
        schemaEngine.createSchemaRegion(schemaRegionId);
      }
    }
  }

  public synchronized void clear() {
    table.clear();
    schemaEngine = null;
  }

  public ISchemaRegionId getSchemaRegionIdWithAutoCreate(PartialPath path)
      throws MetadataException {
    PartialPath storageGroup = storageGroupSchemaManager.getBelongedStorageGroup(path);
    ISchemaRegionId schemaRegionId = calculateSchemaRegionId(storageGroup, path);
    if (!table.containsKey(storageGroup) || !table.get(storageGroup).contains(schemaRegionId)) {
      createSchemaRegion(storageGroup, schemaRegionId);
    }
    return schemaRegionId;
  }

  public List<ISchemaRegionId> getInvolvedSchemaRegionIds(
      PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException {
    List<ISchemaRegionId> result = new ArrayList<>();
    for (PartialPath storageGroup :
        storageGroupSchemaManager.getInvolvedStorageGroups(pathPattern, isPrefixMatch)) {
      if (table.containsKey(storageGroup)) {
        result.addAll(table.get(storageGroup));
      }
    }
    return result;
  }

  public Set<ISchemaRegionId> getSchemaRegionIdsByStorageGroup(PartialPath storageGroup) {
    return table.get(storageGroup);
  }

  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    table.put(storageGroup, Collections.synchronizedSet(new HashSet<>()));
  }

  public void deleteStorageGroup(PartialPath storageGroup) throws MetadataException {
    schemaEngine.deleteStorageGroup(storageGroup, table.remove(storageGroup));
  }

  private synchronized void createSchemaRegion(
      PartialPath storageGroup, ISchemaRegionId schemaRegionId) throws MetadataException {
    synchronized (table) {
      schemaEngine.createSchemaRegion(schemaRegionId);
      table.get(storageGroup).add(schemaRegionId);
    }
  }

  private ISchemaRegionId calculateSchemaRegionId(PartialPath storageGroup, PartialPath path) {
    return ISchemaRegionId.getISchemaRegionId(
        storageGroup.toString(), storageGroup.toString() + "_schema_region");
  }
}
