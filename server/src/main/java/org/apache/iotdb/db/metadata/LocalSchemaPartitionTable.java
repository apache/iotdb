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

package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegionId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// This class is used for schema partition maintaining the map between storage group and
// schemaRegionIds.
public class LocalSchemaPartitionTable {

  private Map<PartialPath, Set<ISchemaRegionId>> table;

  private static class LocalSchemaPartitionTableHolder {
    private static final LocalSchemaPartitionTable INSTANCE = new LocalSchemaPartitionTable();

    private LocalSchemaPartitionTableHolder() {};
  }

  private LocalSchemaPartitionTable() {}

  public static LocalSchemaPartitionTable getInstance() {
    return LocalSchemaPartitionTableHolder.INSTANCE;
  }

  public synchronized void init() throws MetadataException {
    table = new ConcurrentHashMap<>();
  }

  public synchronized void clear() {
    if (table != null) {
      table.clear();
      table = null;
    }
  }

  public synchronized void putSchemaRegionId(
      PartialPath storageGroup, ISchemaRegionId schemaRegionId) {
    table.get(storageGroup).add(schemaRegionId);
  }

  public synchronized void removeSchemaRegionId(
      PartialPath storageGroup, ISchemaRegionId schemaRegionId) {
    table.get(storageGroup).remove(schemaRegionId);
  }

  public ISchemaRegionId getSchemaRegionId(PartialPath storageGroup, PartialPath path) {
    return calculateSchemaRegionId(storageGroup, path);
  }

  public List<ISchemaRegionId> getInvolvedSchemaRegionIds(
      PartialPath storageGroup, PartialPath pathPattern, boolean isPrefixMatch) {
    List<ISchemaRegionId> result = new ArrayList<>();
    if (table.containsKey(storageGroup)) {
      result.addAll(table.get(storageGroup));
    }
    return result;
  }

  public Set<ISchemaRegionId> getSchemaRegionIdsByStorageGroup(PartialPath storageGroup) {
    return table.get(storageGroup);
  }

  public synchronized void setStorageGroup(PartialPath storageGroup) {
    table.put(storageGroup, Collections.synchronizedSet(new HashSet<>()));
  }

  public synchronized Set<ISchemaRegionId> deleteStorageGroup(PartialPath storageGroup) {
    return table.remove(storageGroup);
  }

  // This method may be extended to implement multi schemaRegion for one storageGroup
  private ISchemaRegionId calculateSchemaRegionId(PartialPath storageGroup, PartialPath path) {
    return ISchemaRegionId.getISchemaRegionId(
        storageGroup.toString(), storageGroup.toString() + "_schema_region");
  }
}
