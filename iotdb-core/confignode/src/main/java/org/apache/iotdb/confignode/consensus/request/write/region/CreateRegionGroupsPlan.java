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

package org.apache.iotdb.confignode.consensus.request.write.region;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.i18n.ManagerMessages;

import org.slf4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

/** Create regions for specified Databases. */
public class CreateRegionGroupsPlan extends ConfigPhysicalPlan {

  public static final long DATABASE_GENERATION_NOT_SET = -1;

  // Map<Database, List<TRegionReplicaSet>>
  protected final Map<String, List<TRegionReplicaSet>> regionGroupMap;

  // Map<Database, lifecycle generation>. It fences a RegionGroup allocation from a later database
  // that reuses the same name.
  protected final Map<String, Long> databaseGenerationMap;

  public CreateRegionGroupsPlan() {
    super(ConfigPhysicalPlanType.CreateRegionGroups);
    this.regionGroupMap = new HashMap<>();
    this.databaseGenerationMap = new HashMap<>();
  }

  public CreateRegionGroupsPlan(final ConfigPhysicalPlanType type) {
    super(type);
    this.regionGroupMap = new HashMap<>();
    this.databaseGenerationMap = new HashMap<>();
  }

  public Map<String, List<TRegionReplicaSet>> getRegionGroupMap() {
    return regionGroupMap;
  }

  public Map<String, Long> getDatabaseGenerationMap() {
    return databaseGenerationMap;
  }

  public long getDatabaseGeneration(final String database) {
    return databaseGenerationMap.getOrDefault(database, DATABASE_GENERATION_NOT_SET);
  }

  public boolean isDatabaseGenerationSet(final String database) {
    return databaseGenerationMap.containsKey(database);
  }

  public void setDatabaseGeneration(final String database, final long databaseGeneration) {
    databaseGenerationMap.put(database, databaseGeneration);
  }

  public void addRegionGroup(final String database, final TRegionReplicaSet regionReplicaSet) {
    regionGroupMap
        .computeIfAbsent(database, regionReplicaSets -> new ArrayList<>())
        .add(regionReplicaSet);
  }

  public void planLog(final Logger logger) {
    for (final Map.Entry<String, List<TRegionReplicaSet>> regionGroupEntry :
        regionGroupMap.entrySet()) {
      final String database = regionGroupEntry.getKey();
      for (final TRegionReplicaSet regionReplicaSet : regionGroupEntry.getValue()) {
        logger.info(
            ManagerMessages
                .LOG_CREATEREGIONGROUPS_REGIONGROUP_ARG_BELONGED_DATABASE_ARG_DATANODES_ARG_5270AB6B,
            regionReplicaSet.getRegionId(),
            database,
            regionReplicaSet.getDataNodeLocations().stream()
                .map(TDataNodeLocation::getDataNodeId)
                .collect(Collectors.toList()));
      }
    }
  }

  public void serializeForProcedure(final DataOutputStream stream) throws IOException {
    serializeRegionGroupMap(stream);
  }

  public void deserializeForProcedure(final ByteBuffer buffer) throws IOException {
    // to remove the planType of ConfigPhysicalPlanType
    buffer.getShort();
    deserializeRegionGroupMap(buffer);
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    serializeRegionGroupMap(stream);
    serializeDatabaseGenerationMap(stream);
  }

  private void serializeRegionGroupMap(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(regionGroupMap.size());
    for (final Entry<String, List<TRegionReplicaSet>> entry : regionGroupMap.entrySet()) {
      final String database = entry.getKey();
      final List<TRegionReplicaSet> regionReplicaSets = entry.getValue();
      BasicStructureSerDeUtil.write(database, stream);
      stream.writeInt(regionReplicaSets.size());
      regionReplicaSets.forEach(
          regionReplicaSet ->
              ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet, stream));
    }
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    deserializeRegionGroupMap(buffer);
    if (buffer.hasRemaining()) {
      deserializeDatabaseGenerationMap(buffer);
    }
  }

  private void deserializeRegionGroupMap(final ByteBuffer buffer) throws IOException {
    final int databaseNum = buffer.getInt();
    for (int i = 0; i < databaseNum; i++) {
      final String database = BasicStructureSerDeUtil.readString(buffer);
      regionGroupMap.put(database, new ArrayList<>());

      final int regionReplicaSetNum = buffer.getInt();
      for (int j = 0; j < regionReplicaSetNum; j++) {
        final TRegionReplicaSet regionReplicaSet =
            ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(buffer);
        regionGroupMap.get(database).add(regionReplicaSet);
      }
    }
  }

  public void serializeDatabaseGenerationMap(final DataOutputStream stream) throws IOException {
    stream.writeInt(databaseGenerationMap.size());
    for (final Entry<String, Long> entry : databaseGenerationMap.entrySet()) {
      BasicStructureSerDeUtil.write(entry.getKey(), stream);
      stream.writeLong(entry.getValue());
    }
  }

  public void deserializeDatabaseGenerationMap(final ByteBuffer buffer) {
    final int databaseNum = buffer.getInt();
    for (int i = 0; i < databaseNum; i++) {
      databaseGenerationMap.put(BasicStructureSerDeUtil.readString(buffer), buffer.getLong());
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final CreateRegionGroupsPlan that = (CreateRegionGroupsPlan) o;
    return Objects.equals(regionGroupMap, that.regionGroupMap)
        && Objects.equals(databaseGenerationMap, that.databaseGenerationMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), regionGroupMap, databaseGenerationMap);
  }
}
