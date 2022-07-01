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
package org.apache.iotdb.confignode.persistence.executor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.ActivateDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.AdjustMaxRegionGroupCountPlan;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.DropFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ConfigPlanExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPlanExecutor.class);

  private final NodeInfo nodeInfo;

  private final ClusterSchemaInfo clusterSchemaInfo;

  private final PartitionInfo partitionInfo;

  private final AuthorInfo authorInfo;

  private final ProcedureInfo procedureInfo;

  private final UDFInfo udfInfo;

  public ConfigPlanExecutor(
      NodeInfo nodeInfo,
      ClusterSchemaInfo clusterSchemaInfo,
      PartitionInfo partitionInfo,
      AuthorInfo authorInfo,
      ProcedureInfo procedureInfo,
      UDFInfo udfInfo) {
    this.nodeInfo = nodeInfo;
    this.clusterSchemaInfo = clusterSchemaInfo;
    this.partitionInfo = partitionInfo;
    this.authorInfo = authorInfo;
    this.procedureInfo = procedureInfo;
    this.udfInfo = udfInfo;
  }

  public DataSet executeQueryPlan(ConfigPhysicalPlan req)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (req.getType()) {
      case GetDataNodeInfo:
        return nodeInfo.getDataNodeInfo((GetDataNodeInfoPlan) req);
      case CountStorageGroup:
        return clusterSchemaInfo.countMatchedStorageGroups((CountStorageGroupPlan) req);
      case GetStorageGroup:
        return clusterSchemaInfo.getMatchedStorageGroupSchemas((GetStorageGroupPlan) req);
      case GetDataPartition:
      case GetOrCreateDataPartition:
        return partitionInfo.getDataPartition((GetDataPartitionPlan) req);
      case GetSchemaPartition:
      case GetOrCreateSchemaPartition:
        return partitionInfo.getSchemaPartition((GetSchemaPartitionPlan) req);
      case ListUser:
        return authorInfo.executeListUser();
      case ListRole:
        return authorInfo.executeListRole();
      case ListUserPrivilege:
        return authorInfo.executeListUserPrivileges((AuthorPlan) req);
      case ListRolePrivilege:
        return authorInfo.executeListRolePrivileges((AuthorPlan) req);
      case ListUserRoles:
        return authorInfo.executeListUserRoles((AuthorPlan) req);
      case ListRoleUsers:
        return authorInfo.executeListRoleUsers((AuthorPlan) req);
      case GetNodePathsPartition:
        return getSchemaNodeManagementPartition(req);
      case GetRegionInfoList:
        return partitionInfo.getRegionInfoList((GetRegionInfoListPlan) req);
      default:
        throw new UnknownPhysicalPlanTypeException(req.getType());
    }
  }

  public TSStatus executeNonQueryPlan(ConfigPhysicalPlan req)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (req.getType()) {
      case RegisterDataNode:
        return nodeInfo.registerDataNode((RegisterDataNodePlan) req);
      case ActivateDataNode:
        return nodeInfo.activateDataNode((ActivateDataNodePlan) req);
      case SetStorageGroup:
        TSStatus status = clusterSchemaInfo.setStorageGroup((SetStorageGroupPlan) req);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return status;
        }
        return partitionInfo.setStorageGroup((SetStorageGroupPlan) req);
      case AdjustMaxRegionGroupCount:
        return clusterSchemaInfo.adjustMaxRegionGroupCount((AdjustMaxRegionGroupCountPlan) req);
      case DeleteStorageGroup:
        partitionInfo.deleteStorageGroup((DeleteStorageGroupPlan) req);
        return clusterSchemaInfo.deleteStorageGroup((DeleteStorageGroupPlan) req);
      case PreDeleteStorageGroup:
        return partitionInfo.preDeleteStorageGroup((PreDeleteStorageGroupPlan) req);
      case SetTTL:
        return clusterSchemaInfo.setTTL((SetTTLPlan) req);
      case SetSchemaReplicationFactor:
        return clusterSchemaInfo.setSchemaReplicationFactor((SetSchemaReplicationFactorPlan) req);
      case SetDataReplicationFactor:
        return clusterSchemaInfo.setDataReplicationFactor((SetDataReplicationFactorPlan) req);
      case SetTimePartitionInterval:
        return clusterSchemaInfo.setTimePartitionInterval((SetTimePartitionIntervalPlan) req);
      case CreateRegionGroups:
        return partitionInfo.createRegionGroups((CreateRegionsPlan) req);
      case CreateSchemaPartition:
        return partitionInfo.createSchemaPartition((CreateSchemaPartitionPlan) req);
      case CreateDataPartition:
        return partitionInfo.createDataPartition((CreateDataPartitionPlan) req);
      case UpdateProcedure:
        return procedureInfo.updateProcedure((UpdateProcedurePlan) req);
      case DeleteProcedure:
        return procedureInfo.deleteProcedure((DeleteProcedurePlan) req);
      case CreateUser:
      case CreateRole:
      case DropUser:
      case DropRole:
      case GrantRole:
      case GrantUser:
      case GrantRoleToUser:
      case RevokeUser:
      case RevokeRole:
      case RevokeRoleFromUser:
      case UpdateUser:
        return authorInfo.authorNonQuery((AuthorPlan) req);
      case ApplyConfigNode:
        return nodeInfo.applyConfigNode((ApplyConfigNodePlan) req);
      case RemoveConfigNode:
        return nodeInfo.removeConfigNode((RemoveConfigNodePlan) req);
      case CreateFunction:
        return udfInfo.createFunction((CreateFunctionPlan) req);
      case DropFunction:
        return udfInfo.dropFunction((DropFunctionPlan) req);
      default:
        throw new UnknownPhysicalPlanTypeException(req.getType());
    }
  }

  public boolean takeSnapshot(File snapshotDir) {

    // consensus layer needs to ensure that the directory exists.
    // if it does not exist, print a log to warn there may have a problem.
    if (!snapshotDir.exists()) {
      LOGGER.warn(
          "snapshot directory [{}] is not exist,start to create it.",
          snapshotDir.getAbsolutePath());
      // try to create a directory to enable snapshot operation
      if (!snapshotDir.mkdirs()) {
        LOGGER.error("snapshot directory [{}] can not be created.", snapshotDir.getAbsolutePath());
        return false;
      }
    }

    // If the directory is not empty, we should not continue the snapshot operation,
    // which may result in incorrect results.
    File[] fileList = snapshotDir.listFiles();
    if (fileList != null && fileList.length > 0) {
      LOGGER.error("snapshot directory [{}] is not empty.", snapshotDir.getAbsolutePath());
      return false;
    }

    AtomicBoolean result = new AtomicBoolean(true);
    getAllAttributes()
        .parallelStream()
        .forEach(
            x -> {
              boolean takeSnapshotResult = true;
              try {
                takeSnapshotResult = x.processTakeSnapshot(snapshotDir);
              } catch (TException | IOException e) {
                LOGGER.error(e.getMessage());
                takeSnapshotResult = false;
              } finally {
                // If any snapshot fails, the whole fails
                // So this is just going to be false
                if (!takeSnapshotResult) {
                  result.set(false);
                }
              }
            });
    return result.get();
  }

  public void loadSnapshot(File latestSnapshotRootDir) {
    if (!latestSnapshotRootDir.exists()) {
      LOGGER.error(
          "snapshot directory [{}] is not exist, can not load snapshot with this directory.",
          latestSnapshotRootDir.getAbsolutePath());
      return;
    }

    getAllAttributes()
        .parallelStream()
        .forEach(
            x -> {
              try {
                x.processLoadSnapshot(latestSnapshotRootDir);
              } catch (TException | IOException e) {
                LOGGER.error(e.getMessage());
              }
            });
  }

  private DataSet getSchemaNodeManagementPartition(ConfigPhysicalPlan req) {
    int level;
    PartialPath partialPath;
    Set<String> alreadyMatchedNode;
    Set<PartialPath> needMatchedNode;
    List<String> matchedStorageGroups = new ArrayList<>();

    GetNodePathsPartitionPlan getNodePathsPartitionPlan = (GetNodePathsPartitionPlan) req;
    partialPath = getNodePathsPartitionPlan.getPartialPath();
    level = getNodePathsPartitionPlan.getLevel();
    if (-1 == level) {
      // get child paths
      Pair<Set<String>, Set<PartialPath>> matchedChildInNextLevel =
          clusterSchemaInfo.getChildNodePathInNextLevel(partialPath);
      alreadyMatchedNode = matchedChildInNextLevel.left;
      needMatchedNode = matchedChildInNextLevel.right;
    } else {
      // count nodes
      Pair<List<PartialPath>, Set<PartialPath>> matchedChildInNextLevel =
          clusterSchemaInfo.getNodesListInGivenLevel(partialPath, level);
      alreadyMatchedNode =
          matchedChildInNextLevel.left.stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet());
      needMatchedNode = matchedChildInNextLevel.right;
    }

    needMatchedNode.forEach(nodePath -> matchedStorageGroups.add(nodePath.getFullPath()));
    SchemaNodeManagementResp schemaNodeManagementResp =
        (SchemaNodeManagementResp)
            partitionInfo.getSchemaNodeManagementPartition(matchedStorageGroups);
    if (schemaNodeManagementResp.getStatus().getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      schemaNodeManagementResp.setMatchedNode(alreadyMatchedNode);
    }
    return schemaNodeManagementResp;
  }

  private List<SnapshotProcessor> getAllAttributes() {
    List<SnapshotProcessor> allAttributes = new ArrayList<>();
    allAttributes.add(clusterSchemaInfo);
    allAttributes.add(partitionInfo);
    allAttributes.add(nodeInfo);
    allAttributes.add(udfInfo);
    return allAttributes;
  }
}
