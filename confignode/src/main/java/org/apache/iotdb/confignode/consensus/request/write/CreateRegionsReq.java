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
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Create regions for specific StorageGroup */
public class CreateRegionsReq extends ConfigRequest {

  private final List<TRegionReplicaSet> regionReplicaSets;

  public CreateRegionsReq() {
    super(ConfigRequestType.CreateRegions);
    this.regionReplicaSets = new ArrayList<>();
  }

  public void addRegion(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSets.add(regionReplicaSet);
  }

  public List<TRegionReplicaSet> getRegionReplicaSets() {
    return regionReplicaSets;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(ConfigRequestType.CreateRegions.ordinal());

    buffer.putInt(regionReplicaSets.size());
    for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
      ThriftCommonsSerDeUtils.writeTRegionReplicaSet(regionReplicaSet, buffer);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      regionReplicaSets.add(ThriftCommonsSerDeUtils.readTRegionReplicaSet(buffer));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateRegionsReq that = (CreateRegionsReq) o;
    return regionReplicaSets.equals(that.regionReplicaSets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionReplicaSets);
  }
}
