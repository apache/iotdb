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

package org.apache.iotdb.db.mpp.execution.executor;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.partition.ExecutorType;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.engine.storagegroup.VirtualDataRegion;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionReadExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionReadExecutor.class);

  public RegionExecutionResult execute(
      ExecutorType executorType, FragmentInstance fragmentInstance) {
    // execute fragment instance in state machine
    ConsensusReadResponse readResponse;
    try (SetThreadName threadName = new SetThreadName(fragmentInstance.getId().getFullId())) {
      RegionExecutionResult resp = new RegionExecutionResult();
      if (executorType.isStorageExecutor()) {
        // FI with storageExecutor will be executed by Consensus
        ConsensusGroupId groupId;
        try {
          groupId =
              ConsensusGroupId.Factory.createFromTConsensusGroupId(
                  fragmentInstance.getRegionReplicaSet().getRegionId());
        } catch (Throwable t) {
          LOGGER.warn("Deserialize ConsensusGroupId failed. ", t);
          throw new FragmentInstanceDispatchException(
              RpcUtils.getStatus(
                  TSStatusCode.EXECUTE_STATEMENT_ERROR,
                  "Deserialize ConsensusGroupId failed: " + t.getMessage()));
        }
        if (groupId instanceof DataRegionId) {
          readResponse = DataRegionConsensusImpl.getInstance().read(groupId, fragmentInstance);
        } else {
          readResponse = SchemaRegionConsensusImpl.getInstance().read(groupId, fragmentInstance);
        }
        if (readResponse == null) {
          LOGGER.error("ReadResponse is null");
          resp.setAccepted(false);
          resp.setMessage("ReadResponse is null");
        } else if (!readResponse.isSuccess()) {
          LOGGER.error(
              "Execute FragmentInstance in ConsensusGroup {} failed.",
              groupId,
              readResponse.getException());
          resp.setAccepted(false);
          resp.setMessage(
              "Execute FragmentInstance failed: "
                  + (readResponse.getException() == null
                      ? ""
                      : readResponse.getException().getMessage()));
        } else {
          FragmentInstanceInfo info = (FragmentInstanceInfo) readResponse.getDataset();
          resp.setAccepted(!info.getState().isFailed());
          resp.setMessage(info.getMessage());
        }
      } else {
        // FI with queryExecutor will be executed directly
        FragmentInstanceInfo info =
            FragmentInstanceManager.getInstance()
                .execDataQueryFragmentInstance(fragmentInstance, VirtualDataRegion.getInstance());
        resp.setAccepted(!info.getState().isFailed());
        resp.setMessage(info.getMessage());
      }
      return resp;
    } catch (Throwable t) {
      LOGGER.error("Execute FragmentInstance in ConsensusGroup {} failed.", executorType, t);
      RegionExecutionResult resp = new RegionExecutionResult();
      resp.setAccepted(false);
      resp.setMessage("Execute FragmentInstance failed: " + t.getMessage());
      return resp;
    }
  }
}
