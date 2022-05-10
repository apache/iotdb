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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Create regions for specific StorageGroups */
public class CreateRegionsReq extends ConfigRequest {

  private final Map<String, TRegionReplicaSet> regionMap;

  public CreateRegionsReq() {
    super(ConfigRequestType.CreateRegions);
    this.regionMap = new HashMap<>();
  }

  public Map<String, TRegionReplicaSet> getRegionMap() {
    return regionMap;
  }

  public void addRegion(String storageGroup, TRegionReplicaSet regionReplicaSet) {
    regionMap.put(storageGroup, regionReplicaSet);
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(ConfigRequestType.CreateRegions.ordinal());

    buffer.putInt(regionMap.size());
    regionMap.forEach((storageGroup, regionReplicaSet) -> {
      BasicStructureSerDeUtil.write(storageGroup, buffer);
      ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet, buffer);
    });
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      String storageGroup = BasicStructureSerDeUtil.readString(buffer);
      TRegionReplicaSet regionReplicaSet = ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(buffer);
      regionMap.put(storageGroup, regionReplicaSet);
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
