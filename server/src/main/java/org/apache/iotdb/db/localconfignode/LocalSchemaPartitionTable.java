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

package org.apache.iotdb.db.localconfignode;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

// This class is used for schema partition maintaining the map between database and
// schemaRegionIds.
public class LocalSchemaPartitionTable {

  private AtomicInteger schemaRegionIdGenerator;

  private Map<PartialPath, List<SchemaRegionId>> table;

  private static class LocalSchemaPartitionTableHolder {
    private static final LocalSchemaPartitionTable INSTANCE = new LocalSchemaPartitionTable();

    private LocalSchemaPartitionTableHolder() {};
  }

  private LocalSchemaPartitionTable() {}

  public static LocalSchemaPartitionTable getInstance() {
    return LocalSchemaPartitionTableHolder.INSTANCE;
  }

  public synchronized void init(
      Map<PartialPath, List<SchemaRegionId>> recoveredLocalSchemaRegionInfo)
      throws MetadataException {
    table = new ConcurrentHashMap<>();
    schemaRegionIdGenerator = new AtomicInteger(0);
    for (PartialPath storageGroup : recoveredLocalSchemaRegionInfo.keySet()) {
      List<SchemaRegionId> schemaRegionIdList = new CopyOnWriteArrayList<>();
      table.put(storageGroup, schemaRegionIdList);
      for (SchemaRegionId schemaRegionId : recoveredLocalSchemaRegionInfo.get(storageGroup)) {
        schemaRegionIdList.add(schemaRegionId);

        if (schemaRegionId.getId() >= schemaRegionIdGenerator.get()) {
          schemaRegionIdGenerator.set(schemaRegionId.getId() + 1);
        }
      }
    }
  }

  public synchronized void clear() {
    if (table != null) {
      table.clear();
      table = null;
    }

    if (schemaRegionIdGenerator != null) {
      schemaRegionIdGenerator = null;
    }
  }

  public SchemaRegionId getSchemaRegionId(PartialPath storageGroup, PartialPath path) {
    if (!table.containsKey(storageGroup)) {
      return null;
    }
    return calculateSchemaRegionId(storageGroup, path);
  }

  public List<SchemaRegionId> getInvolvedSchemaRegionIds(
      PartialPath storageGroup, PartialPath pathPattern, boolean isPrefixMatch) {
    List<SchemaRegionId> result = new ArrayList<>();
    if (table.containsKey(storageGroup)) {
      result.addAll(table.get(storageGroup));
    }
    return result;
  }

  public List<SchemaRegionId> getSchemaRegionIdsByStorageGroup(PartialPath storageGroup) {
    return new ArrayList<>(table.get(storageGroup));
  }

  public synchronized List<SchemaRegionId> setStorageGroup(PartialPath storageGroup) {
    if (table.containsKey(storageGroup)) {
      return table.get(storageGroup);
    }
    List<SchemaRegionId> schemaRegionIdList = new CopyOnWriteArrayList<>();
    schemaRegionIdList.add(new SchemaRegionId(schemaRegionIdGenerator.getAndIncrement()));
    table.put(storageGroup, schemaRegionIdList);
    return schemaRegionIdList;
  }

  public synchronized List<SchemaRegionId> deleteStorageGroup(PartialPath storageGroup) {
    return table.remove(storageGroup);
  }

  // This method may be extended to implement multi schemaRegion for one storageGroup
  // todo keep consistent with the partition method of config node in new cluster
  private SchemaRegionId calculateSchemaRegionId(PartialPath storageGroup, PartialPath path) {
    return table.get(storageGroup).get(0);
  }
}
