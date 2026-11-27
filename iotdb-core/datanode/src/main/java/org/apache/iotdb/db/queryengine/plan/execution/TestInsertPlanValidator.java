/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.List;

/** Checks reception and routing only; never invokes triggers, consensus or storage writes. */
public class TestInsertPlanValidator {
  private TestInsertPlanValidator() {}

  public static TSendBatchPlanNodeResp validate(TSendBatchPlanNodeReq request) {
    List<TSendSinglePlanNodeResp> responses = new ArrayList<>();
    for (TSendSinglePlanNodeReq item : request.getRequests()) {
      TSStatus status;
      try {
        PlanNode node = PlanNodeType.deserialize(item.getPlanNode().bufferForBody());
        if (!(node instanceof InsertNode)
            || item.getConsensusGroupId().getType() != TConsensusGroupType.DataRegion) {
          status =
              RpcUtils.getStatus(
                  TSStatusCode.EXECUTE_STATEMENT_ERROR,
                  DataNodeQueryMessages
                      .MESSAGE_TEST_INSERTION_ACCEPTS_ONLY_INSERTION_PLANS_TARGETING_DATA_REGIONS_319E02A6);
        } else if (DataNodeRegionManager.getInstance()
                .getRegionLock(
                    ConsensusGroupId.Factory.createFromTConsensusGroupId(
                        item.getConsensusGroupId()))
            == null) {
          status =
              RpcUtils.getStatus(
                  TSStatusCode.NO_AVAILABLE_REGION_GROUP,
                  DataNodeQueryMessages
                      .MESSAGE_TEST_INSERTION_TARGET_REGION_DOES_NOT_EXIST_A4645EC1);
        } else {
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
        }
      } catch (RuntimeException e) {
        status = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
      }
      TSendSinglePlanNodeResp response = new TSendSinglePlanNodeResp();
      response.setStatus(status);
      response.setAccepted(status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode());
      responses.add(response);
    }
    return new TSendBatchPlanNodeResp(responses);
  }
}
