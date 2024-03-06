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

package org.apache.iotdb.db.consensus.statemachine.schemaregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.BaseStateMachine;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SchemaRegionStateMachine extends BaseStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(SchemaRegionStateMachine.class);

  private final ISchemaRegion schemaRegion;
  private static final FragmentInstanceManager QUERY_INSTANCE_MANAGER =
      FragmentInstanceManager.getInstance();

  public SchemaRegionStateMachine(ISchemaRegion schemaRegion) {
    this.schemaRegion = schemaRegion;
  }

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public void stop() {
    // Stop leader related service for schema pipe
    PipeAgent.runtime().notifySchemaLeaderUnavailable(schemaRegion.getSchemaRegionId());
  }

  @Override
  public void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
    if (schemaRegion.getSchemaRegionId().equals(groupId)
        && newLeaderId != IoTDBDescriptor.getInstance().getConfig().getDataNodeId()) {
      // Shutdown leader related service for schema pipe
      PipeAgent.runtime().notifySchemaLeaderUnavailable(schemaRegion.getSchemaRegionId());
    }
  }

  @Override
  public void notifyLeaderReady() {
    // Activate leader related service for schema pipe
    PipeAgent.runtime().notifySchemaLeaderReady(schemaRegion.getSchemaRegionId());
  }

  @Override
  public boolean isReadOnly() {
    return CommonDescriptor.getInstance().getConfig().isReadOnly();
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return schemaRegion.createSnapshot(snapshotDir)
        && PipeAgent.runtime()
            .schemaListener(schemaRegion.getSchemaRegionId())
            .createSnapshot(snapshotDir);
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    schemaRegion.loadSnapshot(latestSnapshotRootDir);
    PipeAgent.runtime()
        .schemaListener(schemaRegion.getSchemaRegionId())
        .loadSnapshot(latestSnapshotRootDir);
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    try {
      TSStatus result = ((PlanNode) request).accept(new SchemaExecutionVisitor(), schemaRegion);
      if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        PipeAgent.runtime()
            .schemaListener(schemaRegion.getSchemaRegionId())
            .tryListenToNode((PlanNode) request);
      }
      return result;
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    FragmentInstance fragmentInstance;
    try {
      fragmentInstance = getFragmentInstance(request);
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage());
      return null;
    }
    logger.debug(
        "SchemaRegionStateMachine[{}]: Execute read plan: FragmentInstance-{}",
        schemaRegion.getSchemaRegionId(),
        fragmentInstance.getId());
    return QUERY_INSTANCE_MANAGER.execSchemaQueryFragmentInstance(fragmentInstance, schemaRegion);
  }
}
