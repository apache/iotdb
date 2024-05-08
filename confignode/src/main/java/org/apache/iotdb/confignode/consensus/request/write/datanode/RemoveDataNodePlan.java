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
package org.apache.iotdb.confignode.consensus.request.write.datanode;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RemoveDataNodePlan extends ConfigPhysicalPlan {
  private List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();

  public RemoveDataNodePlan() {
    super(ConfigPhysicalPlanType.RemoveDataNode);
  }

  public RemoveDataNodePlan(List<TDataNodeLocation> dataNodeLocations) {
    this();
    this.dataNodeLocations = dataNodeLocations;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeInt(dataNodeLocations.size());
    dataNodeLocations.forEach(
        location -> ThriftCommonsSerDeUtils.serializeTDataNodeLocation(location, stream));
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int dataNodeLocationSize = buffer.getInt();
    for (int i = 0; i < dataNodeLocationSize; i++) {
      dataNodeLocations.add(ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RemoveDataNodePlan that = (RemoveDataNodePlan) o;
    return dataNodeLocations.equals(that.dataNodeLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), dataNodeLocations);
  }

  public List<TDataNodeLocation> getDataNodeLocations() {
    return dataNodeLocations;
  }

  @Override
  public String toString() {
    return "{RemoveDataNodeReq: " + "TDataNodeLocations: " + this.getDataNodeLocations() + "}";
  }
}
