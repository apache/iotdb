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

package org.apache.iotdb.db.consensus.statemachine;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.DeleteRegionNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

public class DataRegionStateMachine extends BaseStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(DataRegionStateMachine.class);

  private static final FragmentInstanceManager QUERY_INSTANCE_MANAGER =
      FragmentInstanceManager.getInstance();

  private final DataRegion region;

  public DataRegionStateMachine(DataRegion region) {
    this.region = region;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return false;
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {}

  @Override
  public TSStatus write(IConsensusRequest request) {
    PlanNode planNode;
    try {
      if (request instanceof IndexedConsensusRequest) {
        planNode =
            getFragmentInstance(((IndexedConsensusRequest) request).getRequest())
                .getFragment()
                .getRoot();
        if (planNode instanceof InsertNode) {
          ((InsertNode) planNode)
              .setCurrentIndex(((IndexedConsensusRequest) request).getCurrentIndex());
          ((InsertNode) planNode)
              .setMinSyncIndex(((IndexedConsensusRequest) request).getMinSyncIndex());
        }
      } else {
        planNode = getFragmentInstance(request).getFragment().getRoot();
      }
      return write(planNode);
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  protected TSStatus write(PlanNode planNode) {
    try {
      if (planNode instanceof InsertRowNode) {
        region.insert((InsertRowNode) planNode);
      } else if (planNode instanceof InsertTabletNode) {
        region.insertTablet((InsertTabletNode) planNode);
      } else if (planNode instanceof InsertRowsNode) {
        region.insert((InsertRowsNode) planNode);
      } else if (planNode instanceof InsertMultiTabletsNode) {
        region.insertTablets((InsertMultiTabletsNode) (planNode));
      } else if (planNode instanceof InsertRowsOfOneDeviceNode) {
        region.insert((InsertRowsOfOneDeviceNode) planNode);
      } else if (planNode instanceof DeleteRegionNode) {
        region.syncDeleteDataFiles();
        StorageEngineV2.getInstance()
            .deleteDataRegion((DataRegionId) ((DeleteRegionNode) planNode).getConsensusGroupId());
      } else {
        logger.error("Unsupported plan node for writing to data region : {}", planNode);
        return StatusUtils.UNSUPPORTED_OPERATION;
      }
    } catch (BatchProcessException e) {
      return RpcUtils.getStatus(Arrays.asList(e.getFailingStatus()));
    } catch (Exception e) {
      logger.error("Error in executing plan node: {}", planNode, e);
      return StatusUtils.EXECUTE_STATEMENT_ERROR;
    }
    return StatusUtils.OK;
  }

  @Override
  protected DataSet read(FragmentInstance fragmentInstance) {
    return QUERY_INSTANCE_MANAGER.execDataQueryFragmentInstance(fragmentInstance, region);
  }
}
