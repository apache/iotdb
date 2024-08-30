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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.storageengine.dataregion.VirtualDataRegion;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
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
    try (SetThreadName threadName = new SetThreadName(fragmentInstance.getId().getFullId())) {
      DataSet readResponse;
      if (groupId instanceof DataRegionId) {
        readResponse = dataRegionConsensus.read(groupId, fragmentInstance);
      } else {
        readResponse = schemaRegionConsensus.read(groupId, fragmentInstance);
      }
      if (readResponse == null) {
        LOGGER.error(RESPONSE_NULL_ERROR_MSG);
        return RegionExecutionResult.create(false, RESPONSE_NULL_ERROR_MSG, null);
      } else {
        FragmentInstanceInfo info = (FragmentInstanceInfo) readResponse;
        RegionExecutionResult resp =
            RegionExecutionResult.create(!info.getState().isFailed(), info.getMessage(), null);
        info.getErrorCode()
            .ifPresent(
                s -> {
                  resp.setStatus(s);
                  resp.setReadNeedRetry(StatusUtils.needRetryHelper(s));
                });
        return resp;
      }
    } catch (ConsensusGroupNotExistException e) {
      LOGGER.warn("Execute FragmentInstance in ConsensusGroup {} failed.", groupId, e);
      RegionExecutionResult resp =
          RegionExecutionResult.create(
              false,
              String.format(ERROR_MSG_FORMAT, e.getMessage()),
              new TSStatus(TSStatusCode.CONSENSUS_GROUP_NOT_EXIST.getStatusCode()));
      resp.setReadNeedRetry(true);
      return resp;
    } catch (Throwable e) {
      LOGGER.warn("Execute FragmentInstance in ConsensusGroup {} failed.", groupId, e);
      RegionExecutionResult resp =
          RegionExecutionResult.create(
              false, String.format(ERROR_MSG_FORMAT, e.getMessage()), null);
      Throwable t = ErrorHandlingUtils.getRootCause(e);
      if (t instanceof ReadException
          || t instanceof ReadIndexException
          || t instanceof NotLeaderException
          || t instanceof ServerNotReadyException) {
        resp.setReadNeedRetry(true);
        resp.setStatus(new TSStatus(TSStatusCode.RATIS_READ_UNAVAILABLE.getStatusCode()));
      }
      return resp;
    }
  }

  @SuppressWarnings("squid:S1181")
  public RegionExecutionResult execute(FragmentInstance fragmentInstance) {
    // execute fragment instance in state machine
    try (SetThreadName threadName = new SetThreadName(fragmentInstance.getId().getFullId())) {
      // FI with queryExecutor will be executed directly
      FragmentInstanceInfo info =
          fragmentInstanceManager.execDataQueryFragmentInstance(
              fragmentInstance, VirtualDataRegion.getInstance());
      return RegionExecutionResult.create(!info.getState().isFailed(), info.getMessage(), null);
    } catch (Throwable t) {
      LOGGER.error("Execute FragmentInstance in QueryExecutor failed.", t);
      return RegionExecutionResult.create(
          false, String.format(ERROR_MSG_FORMAT, t.getMessage()), null);
    }
  }
}
