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

import org.apache.iotdb.commons.partition.ConsensusGroupId;
import org.apache.iotdb.commons.partition.GroupType;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// This class is used for schema partition maintaining the map between storage group and
// schemaRegionIds.
public class LocalSchemaPartitionTable {

  private AtomicInteger schemaRegionIdGenerator;

  private Map<PartialPath, Set<ConsensusGroupId>> table;

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
    schemaRegionIdGenerator = new AtomicInteger(0);
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

  public synchronized ConsensusGroupId allocateSchemaRegionId(PartialPath storageGroup) {
    ConsensusGroupId schemaRegionId = new ConsensusGroupId(GroupType.SchemaRegion, schemaRegionIdGenerator.getAndIncrement());
    table.get(storageGroup).add(schemaRegionId);
    return schemaRegionId;
  }

  public synchronized void putSchemaRegionId(
      PartialPath storageGroup, ConsensusGroupId schemaRegionId) {
    table.get(storageGroup).add(schemaRegionId);

    if (schemaRegionId.getId() >= schemaRegionIdGenerator.get()) {
      schemaRegionIdGenerator.set(schemaRegionId.getId() + 1);
    }
  }

  public synchronized void removeSchemaRegionId(
      PartialPath storageGroup, ConsensusGroupId schemaRegionId) {
    table.get(storageGroup).remove(schemaRegionId);
  }

  public ConsensusGroupId getSchemaRegionId(PartialPath storageGroup, PartialPath path) {
    return calculateSchemaRegionId(storageGroup, path);
  }

  public List<ConsensusGroupId> getInvolvedSchemaRegionIds(
      PartialPath storageGroup, PartialPath pathPattern, boolean isPrefixMatch) {
    List<ConsensusGroupId> result = new ArrayList<>();
    if (table.containsKey(storageGroup)) {
      result.addAll(table.get(storageGroup));
    }
    return result;
  }

  public Set<ConsensusGroupId> getSchemaRegionIdsByStorageGroup(PartialPath storageGroup) {
    return table.get(storageGroup);
  }

  public synchronized void setStorageGroup(PartialPath storageGroup) {
    table.put(storageGroup, Collections.synchronizedSet(new HashSet<>()));
  }

  public synchronized Set<ConsensusGroupId> deleteStorageGroup(PartialPath storageGroup) {
    return table.remove(storageGroup);
  }

  // This method may be extended to implement multi schemaRegion for one storageGroup
  // todo keep consistent with the partition method of config node in new cluster
  private ConsensusGroupId calculateSchemaRegionId(PartialPath storageGroup, PartialPath path) {
    return table.get(storageGroup).iterator().next();
  }
}
