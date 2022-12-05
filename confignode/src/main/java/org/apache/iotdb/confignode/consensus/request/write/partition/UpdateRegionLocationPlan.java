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

public class UpdateRegionLocationPlan extends ConfigPhysicalPlan {
  /*which region*/
  TConsensusGroupId regionId;

  /*remove it from the region's location*/
  TDataNodeLocation oldNode;

  /*add it to the region's location*/
  TDataNodeLocation newNode;

  public UpdateRegionLocationPlan() {
    super(ConfigPhysicalPlanType.UpdateRegionLocation);
  }

  /**
   * Constructor
   *
   * @param regionId update the region location
   * @param oldNode remove the old location
   * @param newNode add the new location
   */
  public UpdateRegionLocationPlan(
      TConsensusGroupId regionId, TDataNodeLocation oldNode, TDataNodeLocation newNode) {
    this();
    this.regionId = regionId;
    this.oldNode = oldNode;
    this.newNode = newNode;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(oldNode, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(newNode, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
    oldNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer);
    newNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer);
  }

  public TConsensusGroupId getRegionId() {
    return regionId;
  }

  public TDataNodeLocation getOldNode() {
    return oldNode;
  }

  public TDataNodeLocation getNewNode() {
    return newNode;
  }
}
