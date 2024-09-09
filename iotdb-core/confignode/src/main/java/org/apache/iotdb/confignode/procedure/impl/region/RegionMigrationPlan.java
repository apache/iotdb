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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;

public class RegionMigrationPlan {
  private TConsensusGroupId regionId;
  private TDataNodeLocation fromDataNode;
  private TDataNodeLocation toDataNode;

  public RegionMigrationPlan(TConsensusGroupId regionId, TDataNodeLocation fromDataNode) {
    this.regionId = regionId;
    this.fromDataNode = fromDataNode;
    // default value is fromDataNode, which means no migration
    this.toDataNode = fromDataNode;
  }

  public static RegionMigrationPlan create(
      TConsensusGroupId regionId, TDataNodeLocation fromDataNode) {
    return new RegionMigrationPlan(regionId, fromDataNode);
  }

  public TConsensusGroupId getRegionId() {
    return regionId;
  }

  public TDataNodeLocation getFromDataNode() {
    return fromDataNode;
  }

  public TDataNodeLocation getToDataNode() {
    return toDataNode;
  }

  public void setToDataNode(TDataNodeLocation toDataNode) {
    this.toDataNode = toDataNode;
  }
}
