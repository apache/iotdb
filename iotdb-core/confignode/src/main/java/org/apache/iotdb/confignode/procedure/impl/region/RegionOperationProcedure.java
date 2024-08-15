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
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class RegionOperationProcedure<TState>
    extends StateMachineProcedure<ConfigNodeProcedureEnv, TState> {

  // regionId -> regionOperationProcedure id
  private static final Map<TConsensusGroupId, Integer> regionOperationsMap = new HashMap<>();

  TConsensusGroupId consensusGroupId;

  RegionOperationProcedure() {
    super();
  }

  RegionOperationProcedure(TConsensusGroupId consensusGroupId) {
    super();
    this.consensusGroupId = consensusGroupId;
    addToMap(consensusGroupId, this.getProcId());
  }

  private static synchronized void addToMap(TConsensusGroupId consensusGroupId, long procId) {
    regionOperationsMap.putIfAbsent(consensusGroupId, 0);
    regionOperationsMap.compute(consensusGroupId, (k,v) -> v+1);
  }

  @Override
  protected void procedureEndHook() {
    synchronized (RegionOperationProcedure.class) {
      regionOperationsMap.computeIfPresent(consensusGroupId, (k,v) -> {
        if (v == 1) {
          return null;
        }
        return v-1;
      });
    }
  }

  public static int getRegionOperations(TConsensusGroupId consensusGroupId) {
    return regionOperationsMap.getOrDefault(consensusGroupId, 0);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    consensusGroupId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
    addToMap(consensusGroupId, this.getProcId());
  }

  public TConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public static void main(String[] args) {
    new RegionMigrateProcedure();
  }
}
