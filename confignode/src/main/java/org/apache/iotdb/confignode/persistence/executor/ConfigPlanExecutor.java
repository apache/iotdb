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
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.AdjustMaxRegionGroupCountPlan;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.DropFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
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
      case GetDataNodeConfiguration:
        return nodeInfo.getDataNodeConfiguration((GetDataNodeConfigurationPlan) req);
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
        return authorInfo.executeListUsers((AuthorPlan) req);
      case ListRole:
        return authorInfo.executeListRoles((AuthorPlan) req);
      case ListUserPrivilege:
        return authorInfo.executeListUserPrivileges((AuthorPlan) req);
      case ListRolePrivilege:
        return authorInfo.executeListRolePrivileges((AuthorPlan) req);
      case GetNodePathsPartition:
        return getSchemaNodeManagementPartition(req);
      case GetRegionInfoList:
        return getRegionInfoList(req);
      case GetAllSchemaTemplate:
        return clusterSchemaInfo.getAllTemplates();
      case GetSchemaTemplate:
        return clusterSchemaInfo.getTemplate((GetSchemaTemplatePlan) req);
      case CheckTemplateSettable:
        return clusterSchemaInfo.checkTemplateSettable((CheckTemplateSettablePlan) req);
      case GetPathsSetTemplate:
        return clusterSchemaInfo.getPathsSetTemplate((GetPathsSetTemplatePlan) req);
      case GetAllTemplateSetInfo:
        return clusterSchemaInfo.getAllTemplateSetInfo();
      default:
        throw new UnknownPhysicalPlanTypeException(req.getType());
    }
  }

  public TSStatus executeNonQueryPlan(ConfigPhysicalPlan physicalPlan)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (physicalPlan.getType()) {
      case RegisterDataNode:
        return nodeInfo.registerDataNode((RegisterDataNodePlan) physicalPlan);
      case RemoveDataNode:
        return nodeInfo.removeDataNode((RemoveDataNodePlan) physicalPlan);
      case SetStorageGroup:
        TSStatus status = clusterSchemaInfo.setStorageGroup((SetStorageGroupPlan) physicalPlan);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return status;
        }
        return partitionInfo.setStorageGroup((SetStorageGroupPlan) physicalPlan);
      case AdjustMaxRegionGroupCount:
        return clusterSchemaInfo.adjustMaxRegionGroupCount(
            (AdjustMaxRegionGroupCountPlan) physicalPlan);
      case DeleteStorageGroup:
        partitionInfo.deleteStorageGroup((DeleteStorageGroupPlan) physicalPlan);
        return clusterSchemaInfo.deleteStorageGroup((DeleteStorageGroupPlan) physicalPlan);
      case PreDeleteStorageGroup:
        return partitionInfo.preDeleteStorageGroup((PreDeleteStorageGroupPlan) physicalPlan);
      case SetTTL:
        return clusterSchemaInfo.setTTL((SetTTLPlan) physicalPlan);
      case SetSchemaReplicationFactor:
        return clusterSchemaInfo.setSchemaReplicationFactor(
            (SetSchemaReplicationFactorPlan) physicalPlan);
      case SetDataReplicationFactor:
        return clusterSchemaInfo.setDataReplicationFactor(
            (SetDataReplicationFactorPlan) physicalPlan);
      case SetTimePartitionInterval:
        return clusterSchemaInfo.setTimePartitionInterval(
            (SetTimePartitionIntervalPlan) physicalPlan);
      case CreateRegionGroups:
        return partitionInfo.createRegionGroups((CreateRegionGroupsPlan) physicalPlan);
      case DeleteRegionGroups:
        return partitionInfo.deleteRegionGroups((DeleteRegionGroupsPlan) physicalPlan);
      case CreateSchemaPartition:
        return partitionInfo.createSchemaPartition((CreateSchemaPartitionPlan) physicalPlan);
      case CreateDataPartition:
        return partitionInfo.createDataPartition((CreateDataPartitionPlan) physicalPlan);
      case UpdateProcedure:
        return procedureInfo.updateProcedure((UpdateProcedurePlan) physicalPlan);
      case DeleteProcedure:
        return procedureInfo.deleteProcedure((DeleteProcedurePlan) physicalPlan);
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
        return authorInfo.authorNonQuery((AuthorPlan) physicalPlan);
      case ApplyConfigNode:
        return nodeInfo.applyConfigNode((ApplyConfigNodePlan) physicalPlan);
      case RemoveConfigNode:
        return nodeInfo.removeConfigNode((RemoveConfigNodePlan) physicalPlan);
      case CreateFunction:
        return udfInfo.createFunction((CreateFunctionPlan) physicalPlan);
      case DropFunction:
        return udfInfo.dropFunction((DropFunctionPlan) physicalPlan);
      case CreateSchemaTemplate:
        return clusterSchemaInfo.createSchemaTemplate((CreateSchemaTemplatePlan) physicalPlan);
      case UpdateRegionLocation:
        return partitionInfo.updateRegionLocation((UpdateRegionLocationPlan) physicalPlan);
      case SetSchemaTemplate:
        return clusterSchemaInfo.setSchemaTemplate((SetSchemaTemplatePlan) physicalPlan);
      default:
        throw new UnknownPhysicalPlanTypeException(physicalPlan.getType());
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
    Set<TSchemaNode> alreadyMatchedNode;
    Set<PartialPath> needMatchedNode;
    List<String> matchedStorageGroups = new ArrayList<>();

    GetNodePathsPartitionPlan getNodePathsPartitionPlan = (GetNodePathsPartitionPlan) req;
    partialPath = getNodePathsPartitionPlan.getPartialPath();
    level = getNodePathsPartitionPlan.getLevel();
    if (-1 == level) {
      // get child paths
      Pair<Set<TSchemaNode>, Set<PartialPath>> matchedChildInNextLevel =
          clusterSchemaInfo.getChildNodePathInNextLevel(partialPath);
      alreadyMatchedNode = matchedChildInNextLevel.left;
      needMatchedNode = matchedChildInNextLevel.right;
    } else {
      // count nodes
      Pair<List<PartialPath>, Set<PartialPath>> matchedChildInNextLevel =
          clusterSchemaInfo.getNodesListInGivenLevel(partialPath, level);
      alreadyMatchedNode =
          matchedChildInNextLevel.left.stream()
              .map(path -> new TSchemaNode(path.getFullPath(), MNodeType.UNIMPLEMENT.getNodeType()))
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

  private DataSet getRegionInfoList(ConfigPhysicalPlan req) {
    final GetRegionInfoListPlan getRegionInfoListPlan = (GetRegionInfoListPlan) req;
    TShowRegionReq showRegionReq = getRegionInfoListPlan.getShowRegionReq();
    if (showRegionReq != null && showRegionReq.isSetStorageGroups()) {
      final List<String> storageGroups = showRegionReq.getStorageGroups();
      final List<String> matchedStorageGroups =
          clusterSchemaInfo.getMatchedStorageGroupSchemasByName(storageGroups).values().stream()
              .map(TStorageGroupSchema::getName)
              .collect(Collectors.toList());
      if (!matchedStorageGroups.isEmpty()) {
        showRegionReq.setStorageGroups(matchedStorageGroups);
      }
    }
    return partitionInfo.getRegionInfoList(getRegionInfoListPlan);
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
