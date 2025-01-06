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
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;

public abstract class RegionOperationProcedure<TState>
    extends StateMachineProcedure<ConfigNodeProcedureEnv, TState> {
  TConsensusGroupId regionId;

  public RegionOperationProcedure() {}

  public RegionOperationProcedure(TConsensusGroupId regionId) {
    this.regionId = regionId;
  }

  public void setRegionId(TConsensusGroupId regionId) {
    this.regionId = regionId;
  }

  public TConsensusGroupId getRegionId() {
    return regionId;
  }

  @Override
  public String toString() {
    return super.toString() + ", regionId=" + regionId;
  }
}
