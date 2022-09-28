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
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.analyze.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.service.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RegionWriteExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionWriteExecutor.class);

  private static final DataNodeRegionManager REGION_MANAGER = DataNodeRegionManager.getInstance();

  public RegionExecutionResult execute(ConsensusGroupId groupId, PlanNode planNode) {
    WritePlanNodeExecutionContext context =
        new WritePlanNodeExecutionContext(groupId, REGION_MANAGER.getRegionLock(groupId));
    WritePlanNodeExecutionVisitor executionVisitor = new WritePlanNodeExecutionVisitor();
    return planNode.accept(executionVisitor, context);
  }

  private static class WritePlanNodeExecutionVisitor
      extends PlanVisitor<RegionExecutionResult, WritePlanNodeExecutionContext> {

    @Override
    public RegionExecutionResult visitPlan(PlanNode node, WritePlanNodeExecutionContext context) {
      RegionExecutionResult response = new RegionExecutionResult();

      ConsensusWriteResponse writeResponse =
          executePlanNodeInConsensusLayer(context.getRegionId(), node);
      // TODO need consider more status
      if (writeResponse.getStatus() != null) {
        response.setAccepted(
            TSStatusCode.SUCCESS_STATUS.getStatusCode() == writeResponse.getStatus().getCode());
        response.setMessage(writeResponse.getStatus().message);
        response.setStatus(writeResponse.getStatus());
      } else {
        LOGGER.error(
            "Something wrong happened while calling consensus layer's write API.",
            writeResponse.getException());
        response.setAccepted(false);
        response.setMessage(writeResponse.getException().getMessage());
        response.setStatus(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR));
      }
      return response;
    }

    private ConsensusWriteResponse executePlanNodeInConsensusLayer(
        ConsensusGroupId groupId, PlanNode planNode) {
      if (groupId instanceof DataRegionId) {
        return DataRegionConsensusImpl.getInstance().write(groupId, planNode);
      } else {
        return SchemaRegionConsensusImpl.getInstance().write(groupId, planNode);
      }
    }

    @Override
    public RegionExecutionResult visitInsertRow(
        InsertRowNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertTablet(
        InsertTabletNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertRows(
        InsertRowsNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertMultiTablets(
        InsertMultiTabletsNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertRowsOfOneDevice(
        InsertRowsOfOneDeviceNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    private RegionExecutionResult executeDataInsert(
        InsertNode insertNode, WritePlanNodeExecutionContext context) {
      RegionExecutionResult response = new RegionExecutionResult();
      // data insertion should be blocked by data deletion, especially when deleting timeseries
      context.getRegionWriteValidationRWLock().readLock().lock();
      try {
        try {
          SchemaValidator.validate(insertNode);
        } catch (SemanticException e) {
          response.setAccepted(false);
          response.setMessage(e.getMessage());
          return response;
        }
        boolean hasFailedMeasurement = insertNode.hasFailedMeasurements();
        String partialInsertMessage = null;
        if (hasFailedMeasurement) {
          partialInsertMessage =
              String.format(
                  "Fail to insert measurements %s caused by %s",
                  insertNode.getFailedMeasurements(), insertNode.getFailedMessages());
          LOGGER.warn(partialInsertMessage);
        }

        ConsensusWriteResponse writeResponse =
            executePlanNodeInConsensusLayer(context.getRegionId(), insertNode);

        // TODO need consider more status
        if (writeResponse.getStatus() != null) {
          response.setAccepted(
              !hasFailedMeasurement
                  && TSStatusCode.SUCCESS_STATUS.getStatusCode()
                      == writeResponse.getStatus().getCode());
          if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != writeResponse.getStatus().getCode()) {
            response.setMessage(writeResponse.getStatus().message);
            response.setStatus(writeResponse.getStatus());
          } else if (hasFailedMeasurement) {
            response.setMessage(partialInsertMessage);
            response.setStatus(
                RpcUtils.getStatus(
                    TSStatusCode.METADATA_ERROR.getStatusCode(), partialInsertMessage));
          } else {
            response.setMessage(writeResponse.getStatus().message);
          }
        } else {
          LOGGER.error(
              "Something wrong happened while calling consensus layer's write API.",
              writeResponse.getException());
          response.setAccepted(false);
          response.setMessage(writeResponse.getException().getMessage());
        }

        return response;
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitDeleteData(
        DeleteDataNode node, WritePlanNodeExecutionContext context) {
      // data deletion should block data insertion, especially when executed for deleting timeseries
      context.getRegionWriteValidationRWLock().writeLock().lock();
      try {
        return super.visitDeleteData(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().writeLock().unlock();
      }
    }
  }

  private static class WritePlanNodeExecutionContext {

    private final ConsensusGroupId regionId;

    private final ReentrantReadWriteLock regionRWLock;

    WritePlanNodeExecutionContext(ConsensusGroupId regionId, ReentrantReadWriteLock regionRWLock) {
      this.regionId = regionId;
      this.regionRWLock = regionRWLock;
    }

    public ConsensusGroupId getRegionId() {
      return regionId;
    }

    public ReentrantReadWriteLock getRegionWriteValidationRWLock() {
      return regionRWLock;
    }
  }
}
