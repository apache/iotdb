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
package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

/** Create regions for specific StorageGroups */
public class CreateRegionsReq extends ConfigRequest {

  private final Map<String, List<TRegionReplicaSet>> regionMap;

  public CreateRegionsReq() {
    super(ConfigRequestType.CreateRegions);
    this.regionMap = new TreeMap<>();
  }

  public Map<String, List<TRegionReplicaSet>> getRegionMap() {
    return regionMap;
  }

  public void addRegion(String storageGroup, TRegionReplicaSet regionReplicaSet) {
    regionMap
        .computeIfAbsent(storageGroup, regionReplicaSets -> new ArrayList<>())
        .add(regionReplicaSet);
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(ConfigRequestType.CreateRegions.ordinal());

    buffer.putInt(regionMap.size());
    regionMap.forEach(
        (storageGroup, regionReplicaSets) -> {
          BasicStructureSerDeUtil.write(storageGroup, buffer);
          buffer.putInt(regionReplicaSets.size());
          regionReplicaSets.forEach(
              regionReplicaSet ->
                  ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet, buffer));
        });
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(ConfigRequestType.CreateRegions.ordinal());

    stream.writeInt(regionMap.size());
    for (Entry<String, List<TRegionReplicaSet>> entry : regionMap.entrySet()) {
      String storageGroup = entry.getKey();
      List<TRegionReplicaSet> regionReplicaSets = entry.getValue();
      BasicStructureSerDeUtil.write(storageGroup, stream);
      stream.writeInt(regionReplicaSets.size());
      regionReplicaSets.forEach(
          regionReplicaSet ->
              ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet, stream));
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int storageGroupNum = buffer.getInt();
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = BasicStructureSerDeUtil.readString(buffer);
      regionMap.put(storageGroup, new ArrayList<>());

      int regionReplicaSetNum = buffer.getInt();
      for (int j = 0; j < regionReplicaSetNum; j++) {
        TRegionReplicaSet regionReplicaSet =
            ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(buffer);
        regionMap.get(storageGroup).add(regionReplicaSet);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateRegionsReq that = (CreateRegionsReq) o;
    return regionMap.equals(that.regionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionMap);
  }
}
