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
package org.apache.iotdb.db.consensus.statemachine.visitor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
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

  @Override
  public TSStatus visitInsertRow(InsertRowNode node, DataRegion dataRegion) {
    try {
      dataRegion.insert(node);
      return StatusUtils.OK;
    } catch (OutOfTTLException e) {
      LOGGER.warn("Error in executing plan node: {}, caused by {}", node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitInsertTablet(InsertTabletNode node, DataRegion dataRegion) {
    try {
      dataRegion.insertTablet(node);
      return StatusUtils.OK;
    } catch (OutOfTTLException e) {
      LOGGER.warn("Error in executing plan node: {}, caused by {}", node, e.getMessage());
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (WriteProcessException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (BatchProcessException e) {
      LOGGER.warn(
          "Batch failure in executing a InsertTabletNode. device: {}, startTime: {}, measurements: {}, failing status: {}",
          node.getDevicePath(),
          node.getTimes()[0],
          node.getMeasurements(),
          e.getFailingStatus());
      return StatusUtils.getStatus(TSStatusCode.representOf(e.getErrorCode()));
    }
  }

  @Override
  public TSStatus visitInsertRows(InsertRowsNode node, DataRegion dataRegion) {
    try {
      dataRegion.insert(node);
      return StatusUtils.OK;
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
            insertRowNode.getDevicePath(),
            insertRowNode.getTime(),
            insertRowNode.getMeasurements(),
            failedEntry.getValue());
      }
      return firstStatus;
    }
  }

  @Override
  public TSStatus visitInsertMultiTablets(InsertMultiTabletsNode node, DataRegion dataRegion) {
    try {
      dataRegion.insertTablets(node);
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
            insertTabletNode.getDevicePath(),
            insertTabletNode.getTimes()[0],
            insertTabletNode.getMeasurements(),
            failedEntry.getValue());
      }
      return firstStatus;
    }
  }

  @Override
  public TSStatus visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceNode node, DataRegion dataRegion) {
    try {
      dataRegion.insert(node);
      return StatusUtils.OK;
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
            insertRowNode.getDevicePath(),
            insertRowNode.getTime(),
            insertRowNode.getMeasurements(),
            failedEntry.getValue());
      }
      return firstStatus;
    }
  }

  @Override
  public TSStatus visitDeleteData(DeleteDataNode node, DataRegion dataRegion) {
    try {
      for (PartialPath path : node.getPathList()) {
        dataRegion.deleteByDevice(
            path, node.getDeleteStartTime(), node.getDeleteEndTime(), node.getSearchIndex(), null);
      }
      return StatusUtils.OK;
    } catch (IOException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return StatusUtils.EXECUTE_STATEMENT_ERROR;
    }
  }
}
