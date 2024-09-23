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

package org.apache.iotdb.confignode.consensus.request.write.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RemoveRegionLocationPlan extends ConfigPhysicalPlan {
  TConsensusGroupId regionId;

  TDataNodeLocation deprecatedLocation;

  public RemoveRegionLocationPlan() {
    super(ConfigPhysicalPlanType.RemoveRegionLocation);
  }

  public RemoveRegionLocationPlan(
      TConsensusGroupId regionId, TDataNodeLocation deprecatedLocation) {
    this();
    this.regionId = regionId;
    this.deprecatedLocation = deprecatedLocation;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(deprecatedLocation, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
    deprecatedLocation = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer);
  }

  public TConsensusGroupId getRegionId() {
    return regionId;
  }

  public TDataNodeLocation getDeprecatedLocation() {
    return deprecatedLocation;
  }
}
