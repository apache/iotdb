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
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaBlacklist;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.DeleteRegionNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InvalidateSchemaCacheNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.rpc.RpcUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

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
    } catch (WriteProcessException | TriggerExecutionException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return StatusUtils.EXECUTE_STATEMENT_ERROR;
    }
  }

  @Override
  public TSStatus visitInsertTablet(InsertTabletNode node, DataRegion dataRegion) {
    try {
      dataRegion.insertTablet(node);
      return StatusUtils.OK;
    } catch (TriggerExecutionException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return StatusUtils.EXECUTE_STATEMENT_ERROR;
    } catch (BatchProcessException e) {
      return RpcUtils.getStatus(Arrays.asList(e.getFailingStatus()));
    }
  }

  @Override
  public TSStatus visitInsertRows(InsertRowsNode node, DataRegion dataRegion) {
    try {
      dataRegion.insert(node);
      return StatusUtils.OK;
    } catch (BatchProcessException e) {
      return RpcUtils.getStatus(Arrays.asList(e.getFailingStatus()));
    }
  }

  @Override
  public TSStatus visitInsertMultiTablets(InsertMultiTabletsNode node, DataRegion dataRegion) {
    try {
      dataRegion.insertTablets(node);
      return StatusUtils.OK;
    } catch (BatchProcessException e) {
      return RpcUtils.getStatus(Arrays.asList(e.getFailingStatus()));
    }
  }

  @Override
  public TSStatus visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceNode node, DataRegion dataRegion) {
    try {
      dataRegion.insert(node);
      return StatusUtils.OK;
    } catch (WriteProcessException | TriggerExecutionException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return StatusUtils.EXECUTE_STATEMENT_ERROR;
    } catch (BatchProcessException e) {
      return RpcUtils.getStatus(Arrays.asList(e.getFailingStatus()));
    }
  }

  @Override
  public TSStatus visitDeleteRegion(DeleteRegionNode node, DataRegion dataRegion) {
    dataRegion.syncDeleteDataFiles();
    StorageEngineV2.getInstance().deleteDataRegion((DataRegionId) node.getConsensusGroupId());
    return StatusUtils.OK;
  }

  @Override
  public TSStatus visitInvalidateSchemaCache(
      InvalidateSchemaCacheNode node, DataRegion dataRegion) {
    String storageGroup = dataRegion.getLogicalStorageGroupName();
    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath path : node.getPathList()) {
      try {
        patternTree.appendPaths(path.alterPrefixPath(new PartialPath(storageGroup)));
      } catch (IllegalPathException e) {
        // this definitely won't happen
        throw new RuntimeException(e);
      }
    }
    DataNodeSchemaBlacklist.getInstance().appendToBlacklist(patternTree);
    return StatusUtils.OK;
  }

  @Override
  public TSStatus visitDeleteData(DeleteDataNode node, DataRegion dataRegion) {
    try {
      for (PartialPath path : node.getPathList()) {
        dataRegion.delete(
            path, node.getDeleteStartTime(), node.getDeleteEndTime(), Long.MAX_VALUE, null);
      }
      return StatusUtils.OK;
    } catch (IOException e) {
      LOGGER.error("Error in executing plan node: {}", node, e);
      return StatusUtils.EXECUTE_STATEMENT_ERROR;
    }
  }
}
