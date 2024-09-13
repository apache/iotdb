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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
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

public class DataExecutionVisitor extends PlanVisitor<TSStatus, DataRegion> {
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
      dataRegion.insertSeparatorToWAL();
      return StatusUtils.OK;
    } catch (OutOfTTLException e) {
      LOGGER.warn("Error in executing plan node: {}, caused by {}", node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessRejectException e) {
      LOGGER.warn("Reject in executing plan node: {}, caused by {}", node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitRelationalInsertTablet(
      RelationalInsertTabletNode node, DataRegion dataRegion) {
    return visitInsertTablet(node, dataRegion);
  }

  @Override
  public TSStatus visitInsertTablet(InsertTabletNode node, DataRegion dataRegion) {
    try {
      dataRegion.insertTablet(node);
      dataRegion.insertSeparatorToWAL();
      return StatusUtils.OK;
    } catch (OutOfTTLException e) {
      LOGGER.warn("Error in executing plan node: {}, caused by {}", node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessRejectException e) {
      LOGGER.warn("Reject in executing plan node: {}, caused by {}", node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (BatchProcessException e) {
      LOGGER.warn(
          "Batch failure in executing a InsertTabletNode. device: {}, startTime: {}, measurements: {}, failing status: {}",
          node.getTargetPath(),
          node.getTimes()[0],
          node.getMeasurements(),
          e.getFailingStatus());
      // For each error
      TSStatus firstStatus = null;
      for (TSStatus status : e.getFailingStatus()) {
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
      dataRegion.insertSeparatorToWAL();
      return StatusUtils.OK;
    } catch (WriteProcessRejectException e) {
      LOGGER.warn("Reject in executing plan node: {}, caused by {}", node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (BatchProcessException e) {
      LOGGER.warn("Batch failure in executing a InsertRowsNode.");
      TSStatus firstStatus = null;
      // for each error
      for (Map.Entry<Integer, TSStatus> failedEntry : node.getResults().entrySet()) {
        InsertRowNode insertRowNode = node.getInsertRowNodeList().get(failedEntry.getKey());
        if (firstStatus == null) {
          firstStatus = failedEntry.getValue();
        }
        LOGGER.warn(
            "Insert row failed. device: {}, time: {}, measurements: {}, failing status: {}",
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
  public TSStatus visitInsertMultiTablets(InsertMultiTabletsNode node, DataRegion dataRegion) {
    try {
      dataRegion.insertTablets(node);
      dataRegion.insertSeparatorToWAL();
      return StatusUtils.OK;
    } catch (BatchProcessException e) {
      LOGGER.warn("Batch failure in executing a InsertMultiTabletsNode.");
      TSStatus firstStatus = null;
      for (Map.Entry<Integer, TSStatus> failedEntry : node.getResults().entrySet()) {
        InsertTabletNode insertTabletNode =
            node.getInsertTabletNodeList().get(failedEntry.getKey());
        if (firstStatus == null) {
          firstStatus = failedEntry.getValue();
        }
        LOGGER.warn(
            "Insert tablet failed. device: {}, startTime: {}, measurements: {}, failing status: {}",
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
      dataRegion.insertSeparatorToWAL();
      return StatusUtils.OK;
    } catch (WriteProcessRejectException e) {
      LOGGER.warn("Reject in executing plan node: {}, caused by {}", node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (BatchProcessException e) {
      LOGGER.warn("Batch failure in executing a InsertRowsOfOneDeviceNode.");
      TSStatus firstStatus = null;
      for (Map.Entry<Integer, TSStatus> failedEntry : node.getResults().entrySet()) {
        InsertRowNode insertRowNode = node.getInsertRowNodeList().get(failedEntry.getKey());
        if (firstStatus == null) {
          firstStatus = failedEntry.getValue();
        }
        LOGGER.warn(
            "Insert row failed. device: {}, time: {}, measurements: {}, failing status: {}",
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
    try {
      for (MeasurementPath path : node.getPathList()) {
        MeasurementPath databaseToDelete =
            new MeasurementPath(dataRegion.getDatabaseName() + ".**");
        if (path.matchFullPath(databaseToDelete)
            || path.getFullPath().equals(databaseToDelete.getFullPath())) {
          LOGGER.info(
              "now try to delete directly, databasePath: {}, deletePath:{}",
              databaseToDelete.getFullPath(),
              path.getFullPath());
          dataRegion.deleteDataDirectly(
              databaseToDelete,
              node.getDeleteStartTime(),
              node.getDeleteEndTime(),
              node.getSearchIndex());
        } else {
          dataRegion.deleteByDevice(
              path, node.getDeleteStartTime(), node.getDeleteEndTime(), node.getSearchIndex());
        }
      }
      dataRegion.insertSeparatorToWAL();
      PipeInsertionDataNodeListener.getInstance().listenToDeleteData(node);
      return StatusUtils.OK;
    } catch (IOException | IllegalPathException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return new TSStatus(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode());
    }
  }

  @Override
  public TSStatus visitPipeEnrichedDeleteDataNode(
      PipeEnrichedDeleteDataNode node, DataRegion context) {
    node.getDeleteDataNode().markAsGeneratedByPipe();
    return visitDeleteData((DeleteDataNode) node.getDeleteDataNode(), context);
  }
}
