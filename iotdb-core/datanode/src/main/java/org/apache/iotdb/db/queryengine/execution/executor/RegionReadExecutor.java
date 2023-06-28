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

package org.apache.iotdb.db.queryengine.execution.executor;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.storageengine.dataregion.VirtualDataRegion;
import org.apache.iotdb.db.utils.SetThreadName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionReadExecutor {

  public static final String RESPONSE_NULL_ERROR_MSG = "ReadResponse is null";

  public static final String ERROR_MSG_FORMAT = "Execute FragmentInstance failed: %s";

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionReadExecutor.class);

  private final IConsensus dataRegionConsensus;

  private final IConsensus schemaRegionConsensus;

  private final FragmentInstanceManager fragmentInstanceManager;

  public RegionReadExecutor() {
    dataRegionConsensus = DataRegionConsensusImpl.getInstance();
    schemaRegionConsensus = SchemaRegionConsensusImpl.getInstance();
    fragmentInstanceManager = FragmentInstanceManager.getInstance();
  }

  @TestOnly
  public RegionReadExecutor(
      IConsensus dataRegionConsensus,
      IConsensus schemaRegionConsensus,
      FragmentInstanceManager fragmentInstanceManager) {
    this.dataRegionConsensus = dataRegionConsensus;
    this.schemaRegionConsensus = schemaRegionConsensus;
    this.fragmentInstanceManager = fragmentInstanceManager;
  }

  @SuppressWarnings("squid:S1181")
  public RegionExecutionResult execute(
      ConsensusGroupId groupId, FragmentInstance fragmentInstance) {
    // execute fragment instance in state machine
    ConsensusReadResponse readResponse;
    try (SetThreadName threadName = new SetThreadName(fragmentInstance.getId().getFullId())) {
      if (groupId instanceof DataRegionId) {
        readResponse = dataRegionConsensus.read(groupId, fragmentInstance);
      } else {
        readResponse = schemaRegionConsensus.read(groupId, fragmentInstance);
      }
      RegionExecutionResult resp = new RegionExecutionResult();
      if (readResponse == null) {
        LOGGER.error(RESPONSE_NULL_ERROR_MSG);
        resp.setAccepted(false);
        resp.setMessage(RESPONSE_NULL_ERROR_MSG);
      } else if (!readResponse.isSuccess()) {
        LOGGER.error(
            "Execute FragmentInstance in ConsensusGroup {} failed.",
            groupId,
            readResponse.getException());
        resp.setAccepted(false);
        resp.setMessage(
            String.format(
                ERROR_MSG_FORMAT,
                readResponse.getException() == null
                    ? ""
                    : readResponse.getException().getMessage()));
      } else {
        FragmentInstanceInfo info = (FragmentInstanceInfo) readResponse.getDataset();
        resp.setAccepted(!info.getState().isFailed());
        resp.setMessage(info.getMessage());
      }
      return resp;
    } catch (Throwable t) {
      LOGGER.error("Execute FragmentInstance in ConsensusGroup {} failed.", groupId, t);
      RegionExecutionResult resp = new RegionExecutionResult();
      resp.setAccepted(false);
      resp.setMessage(String.format(ERROR_MSG_FORMAT, t.getMessage()));
      return resp;
    }
  }

  @SuppressWarnings("squid:S1181")
  public RegionExecutionResult execute(FragmentInstance fragmentInstance) {
    // execute fragment instance in state machine
    try (SetThreadName threadName = new SetThreadName(fragmentInstance.getId().getFullId())) {
      RegionExecutionResult resp = new RegionExecutionResult();
      // FI with queryExecutor will be executed directly
      FragmentInstanceInfo info =
          fragmentInstanceManager.execDataQueryFragmentInstance(
              fragmentInstance, VirtualDataRegion.getInstance());
      resp.setAccepted(!info.getState().isFailed());
      resp.setMessage(info.getMessage());
      return resp;
    } catch (Throwable t) {
      LOGGER.error("Execute FragmentInstance in QueryExecutor failed.", t);
      RegionExecutionResult resp = new RegionExecutionResult();
      resp.setAccepted(false);
      resp.setMessage(String.format(ERROR_MSG_FORMAT, t.getMessage()));
      return resp;
    }
  }
}
