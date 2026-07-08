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

package org.apache.iotdb.db.consensus.statemachine.dataregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.runtime.TableLostRuntimeException;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class DataExecutionVisitor implements PlanVisitor<TSStatus, DataRegion> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataExecutionVisitor.class);

  @Override
  public TSStatus visitPlan(PlanNode node, DataRegion context) {
    return null;
  }

  public TSStatus visitRelationalInsertRow(RelationalInsertRowNode node, DataRegion context) {
    return visitInsertRow(node, context);
  }

  @Override
  public TSStatus visitRelationalInsertRows(RelationalInsertRowsNode node, DataRegion context) {
    return visitInsertRows(node, context);
  }

  @Override
  public TSStatus visitInsertRow(InsertRowNode node, DataRegion dataRegion) {
    try {
      dataRegion.insert(node);
      dataRegion.insertSeparatorToWAL(node);
      return StatusUtils.OK;
    } catch (OutOfTTLException e) {
      LOGGER.warn(DataNodeMiscMessages.ERROR_EXECUTING_PLAN_NODE_CAUSED, node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessRejectException e) {
      LOGGER.warn(DataNodeMiscMessages.REJECT_EXECUTING_PLAN_NODE, node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessException e) {
      LOGGER.error(DataNodeMiscMessages.ERROR_EXECUTING_PLAN_NODE, node, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitRelationalInsertTablet(
      RelationalInsertTabletNode node, DataRegion dataRegion) {
    return visitInsertTablet(node, dataRegion);
  }

  @Override
  public TSStatus visitInsertTablet(final InsertTabletNode node, final DataRegion dataRegion) {
    try {
      dataRegion.insertTablet(node);
      dataRegion.insertSeparatorToWAL(node);
      return StatusUtils.OK;
    } catch (final OutOfTTLException e) {
      LOGGER.debug(DataNodeMiscMessages.ERROR_EXECUTING_PLAN_NODE_CAUSED, node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (final WriteProcessRejectException e) {
      LOGGER.warn(DataNodeMiscMessages.REJECT_EXECUTING_PLAN_NODE, node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (final WriteProcessException e) {
      LOGGER.error(DataNodeMiscMessages.ERROR_EXECUTING_PLAN_NODE, node, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (final BatchProcessException e) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_BATCH_FAILURE_IN_EXECUTING_A_INSERTTABLETNODE_DEVICE_STARTTIME_9A5A70F6,
          node.getTargetPath(),
          node.getTimes()[0],
          node.getMeasurements(),
          e.getFailingStatus());
      // For each error
      TSStatus firstStatus = null;
      for (final TSStatus status : e.getFailingStatus()) {
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          firstStatus = status;
        }
        // Return WRITE_PROCESS_REJECT directly for the consensus retry logic
        if (status.getCode() == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
          return status;
        }
      }
      return firstStatus;
    }
  }

  @Override
  public TSStatus visitInsertRows(InsertRowsNode node, DataRegion dataRegion) {
    try {
      dataRegion.insert(node);
      dataRegion.insertSeparatorToWAL(node);
      return StatusUtils.OK;
    } catch (WriteProcessRejectException e) {
      LOGGER.warn(DataNodeMiscMessages.REJECT_EXECUTING_PLAN_NODE, node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (BatchProcessException e) {
      LOGGER.warn(DataNodeMiscMessages.BATCH_FAILURE_INSERT_ROWS);
      TSStatus firstStatus = null;
      // for each error
      for (Map.Entry<Integer, TSStatus> failedEntry : node.getResults().entrySet()) {
        InsertRowNode insertRowNode = node.getInsertRowNodeList().get(failedEntry.getKey());
        if (firstStatus == null) {
          firstStatus = failedEntry.getValue();
        }
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_INSERT_ROW_FAILED_DEVICE_TIME_MEASUREMENTS_FAILING_STATUS_63054E8B,
            insertRowNode.getTargetPath(),
            insertRowNode.getTime(),
            insertRowNode.getMeasurements(),
            failedEntry.getValue());
        // Return WRITE_PROCESS_REJECT directly for the consensus retry logic
        if (failedEntry.getValue().getCode() == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
          node.clearResults();
          return failedEntry.getValue();
        }
      }
      return firstStatus;
    } catch (SemanticException | TableLostRuntimeException e) {
      LOGGER.error(DataNodeMiscMessages.ERROR_EXECUTING_PLAN_NODE_CAUSED, node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitInsertMultiTablets(InsertMultiTabletsNode node, DataRegion dataRegion) {
    try {
      dataRegion.insertTablets(node);
      dataRegion.insertSeparatorToWAL(node);
      return StatusUtils.OK;
    } catch (WriteProcessRejectException e) {
      LOGGER.warn(DataNodeMiscMessages.REJECT_EXECUTING_PLAN_NODE, node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (BatchProcessException e) {
      LOGGER.warn(DataNodeMiscMessages.BATCH_FAILURE_INSERT_MULTI_TABLETS);
      TSStatus firstStatus = null;
      for (Map.Entry<Integer, TSStatus> failedEntry : node.getResults().entrySet()) {
        InsertTabletNode insertTabletNode =
            node.getInsertTabletNodeList().get(failedEntry.getKey());
        if (firstStatus == null) {
          firstStatus = failedEntry.getValue();
        }
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_INSERT_TABLET_FAILED_DEVICE_STARTTIME_MEASUREMENTS_FAILING_B409B2C4,
            insertTabletNode.getTargetPath(),
            insertTabletNode.getTimes()[0],
            insertTabletNode.getMeasurements(),
            failedEntry.getValue());
        // Return WRITE_PROCESS_REJECT directly for the consensus retry logic
        if (failedEntry.getValue().getCode() == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
          node.clearResults();
          return failedEntry.getValue();
        }
      }
      return firstStatus;
    }
  }

  @Override
  public TSStatus visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceNode node, DataRegion dataRegion) {
    try {
      dataRegion.insert(node);
      dataRegion.insertSeparatorToWAL(node);
      return StatusUtils.OK;
    } catch (WriteProcessRejectException e) {
      LOGGER.warn(DataNodeMiscMessages.REJECT_EXECUTING_PLAN_NODE, node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessException e) {
      LOGGER.error(DataNodeMiscMessages.ERROR_EXECUTING_PLAN_NODE, node, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (BatchProcessException e) {
      LOGGER.warn(DataNodeMiscMessages.BATCH_FAILURE_INSERT_ROWS_ONE_DEVICE);
      TSStatus firstStatus = null;
      for (Map.Entry<Integer, TSStatus> failedEntry : node.getResults().entrySet()) {
        InsertRowNode insertRowNode = node.getInsertRowNodeList().get(failedEntry.getKey());
        if (firstStatus == null) {
          firstStatus = failedEntry.getValue();
        }
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_INSERT_ROW_FAILED_DEVICE_TIME_MEASUREMENTS_FAILING_STATUS_63054E8B,
            insertRowNode.getTargetPath(),
            insertRowNode.getTime(),
            insertRowNode.getMeasurements(),
            failedEntry.getValue());
        // Return WRITE_PROCESS_REJECT directly for the consensus retry logic
        if (failedEntry.getValue().getCode() == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
          node.clearResults();
          return failedEntry.getValue();
        }
      }
      return firstStatus;
    }
  }

  @Override
  public TSStatus visitPipeEnrichedInsertNode(PipeEnrichedInsertNode node, DataRegion context) {
    node.getInsertNode().markAsGeneratedByPipe();
    return node.getInsertNode().accept(this, context);
  }

  @Override
  public TSStatus visitDeleteData(DeleteDataNode node, DataRegion dataRegion) {
    dataRegion.writeLock("deleteData");
    try {
      for (MeasurementPath path : node.getPathList()) {
        MeasurementPath databaseToDelete =
            new MeasurementPath(dataRegion.getDatabaseName() + ".**");
        if (path.matchFullPath(databaseToDelete)
            || path.getFullPath().equals(databaseToDelete.getFullPath())) {
          LOGGER.info(
              DataNodePipeMessages
                  .PIPE_LOG_NOW_TRY_TO_DELETE_DIRECTLY_DATABASEPATH_DELETEPATH_A427CD01,
              databaseToDelete.getFullPath(),
              path.getFullPath());
          dataRegion.deleteDataDirectly(databaseToDelete, node);
        } else {
          dataRegion.deleteByDevice(path, node);
        }
      }
      dataRegion.insertSeparatorToWAL(node);
      return StatusUtils.OK;
    } catch (IOException | IllegalPathException e) {
      LOGGER.error(DataNodeMiscMessages.ERROR_EXECUTING_PLAN_NODE, node, e);
      return new TSStatus(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode());
    } finally {
      dataRegion.writeUnlock();
    }
  }

  @Override
  public TSStatus visitDeleteData(
      final RelationalDeleteDataNode node, final DataRegion dataRegion) {
    try {
      dataRegion.deleteByTable(node);
      dataRegion.insertSeparatorToWAL(node);
      return StatusUtils.OK;
    } catch (final IOException e) {
      LOGGER.error(DataNodeMiscMessages.ERROR_EXECUTING_PLAN_NODE, node, e);
      return new TSStatus(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode());
    }
  }

  @Override
  public TSStatus visitPipeEnrichedDeleteDataNode(
      final PipeEnrichedDeleteDataNode node, final DataRegion context) {
    node.getDeleteDataNode().markAsGeneratedByPipe();
    return node.getDeleteDataNode().accept(this, context);
  }

  @Override
  public TSStatus visitWriteObjectFile(ObjectNode node, DataRegion dataRegion) {
    throw new UnsupportedOperationException();
  }
}
