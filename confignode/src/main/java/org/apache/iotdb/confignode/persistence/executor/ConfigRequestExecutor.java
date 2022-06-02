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
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.ShowPipeReq;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateFunctionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteProcedureReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.DropFunctionReq;
import org.apache.iotdb.confignode.consensus.request.write.OperateReceiverPipeReq;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.request.write.UpdateProcedureReq;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.persistence.ClusterReceiverInfo;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.persistence.PartitionInfo;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.persistence.UDFInfo;
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

public class ConfigRequestExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRequestExecutor.class);

  private final NodeInfo nodeInfo;

  private final ClusterSchemaInfo clusterSchemaInfo;

  private final PartitionInfo partitionInfo;

  private final AuthorInfo authorInfo;

  private final ProcedureInfo procedureInfo;

  private final UDFInfo udfInfo;

  private final ClusterReceiverInfo clusterReceiverInfo;

  public ConfigRequestExecutor(
      NodeInfo nodeInfo,
      ClusterSchemaInfo clusterSchemaInfo,
      PartitionInfo partitionInfo,
      AuthorInfo authorInfo,
      ProcedureInfo procedureInfo,
      UDFInfo udfInfo,
      ClusterReceiverInfo clusterReceiverInfo) {
    this.nodeInfo = nodeInfo;
    this.clusterSchemaInfo = clusterSchemaInfo;
    this.partitionInfo = partitionInfo;
    this.authorInfo = authorInfo;
    this.procedureInfo = procedureInfo;
    this.udfInfo = udfInfo;
    this.clusterReceiverInfo = clusterReceiverInfo;
  }

  public DataSet executorQueryPlan(ConfigRequest req)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (req.getType()) {
      case GetDataNodeInfo:
        return nodeInfo.getDataNodeInfo((GetDataNodeInfoReq) req);
      case CountStorageGroup:
        return clusterSchemaInfo.countMatchedStorageGroups((CountStorageGroupReq) req);
      case GetStorageGroup:
        return clusterSchemaInfo.getMatchedStorageGroupSchemas((GetStorageGroupReq) req);
      case GetDataPartition:
      case GetOrCreateDataPartition:
        return partitionInfo.getDataPartition((GetDataPartitionReq) req);
      case GetSchemaPartition:
      case GetOrCreateSchemaPartition:
        return partitionInfo.getSchemaPartition((GetSchemaPartitionReq) req);
      case ListUser:
        return authorInfo.executeListUser();
      case ListRole:
        return authorInfo.executeListRole();
      case ListUserPrivilege:
        return authorInfo.executeListUserPrivileges((AuthorReq) req);
      case ListRolePrivilege:
        return authorInfo.executeListRolePrivileges((AuthorReq) req);
      case ListUserRoles:
        return authorInfo.executeListUserRoles((AuthorReq) req);
      case ListRoleUsers:
        return authorInfo.executeListRoleUsers((AuthorReq) req);
      case GetNodePathsPartition:
        return getSchemaNodeManagementPartition(req);
      case ShowPipe:
        return clusterReceiverInfo.showPipe((ShowPipeReq) req);
      default:
        throw new UnknownPhysicalPlanTypeException(req.getType());
    }
  }

  public TSStatus executorNonQueryPlan(ConfigRequest req)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (req.getType()) {
      case RegisterDataNode:
        return nodeInfo.registerDataNode((RegisterDataNodeReq) req);
      case SetStorageGroup:
        return clusterSchemaInfo.setStorageGroup((SetStorageGroupReq) req);
      case DeleteStorageGroup:
        partitionInfo.deleteStorageGroup((DeleteStorageGroupReq) req);
        return clusterSchemaInfo.deleteStorageGroup((DeleteStorageGroupReq) req);
      case PreDeleteStorageGroup:
        return partitionInfo.preDeleteStorageGroup((PreDeleteStorageGroupReq) req);
      case SetTTL:
        return clusterSchemaInfo.setTTL((SetTTLReq) req);
      case SetSchemaReplicationFactor:
        return clusterSchemaInfo.setSchemaReplicationFactor((SetSchemaReplicationFactorReq) req);
      case SetDataReplicationFactor:
        return clusterSchemaInfo.setDataReplicationFactor((SetDataReplicationFactorReq) req);
      case SetTimePartitionInterval:
        return clusterSchemaInfo.setTimePartitionInterval((SetTimePartitionIntervalReq) req);
      case CreateRegions:
        TSStatus status = clusterSchemaInfo.createRegions((CreateRegionsReq) req);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return status;
        }
        return partitionInfo.createRegions((CreateRegionsReq) req);
      case DeleteRegions:
        return partitionInfo.deleteRegions((DeleteRegionsReq) req);
      case CreateSchemaPartition:
        return partitionInfo.createSchemaPartition((CreateSchemaPartitionReq) req);
      case CreateDataPartition:
        return partitionInfo.createDataPartition((CreateDataPartitionReq) req);
      case UpdateProcedure:
        return procedureInfo.updateProcedure((UpdateProcedureReq) req);
      case DeleteProcedure:
        return procedureInfo.deleteProcedure((DeleteProcedureReq) req);
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
        return authorInfo.authorNonQuery((AuthorReq) req);
      case ApplyConfigNode:
        return nodeInfo.updateConfigNodeList((ApplyConfigNodeReq) req);
      case CreateFunction:
        return udfInfo.createFunction((CreateFunctionReq) req);
      case DropFunction:
        return udfInfo.dropFunction((DropFunctionReq) req);
      case OperatePipe:
        return clusterReceiverInfo.operatePipe((OperateReceiverPipeReq) req);
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

  private DataSet getSchemaNodeManagementPartition(ConfigRequest req) {
    int level;
    PartialPath partialPath;
    Set<String> alreadyMatchedNode;
    Set<PartialPath> needMatchedNode;
    List<String> matchedStorageGroups = new ArrayList<>();

    GetNodePathsPartitionReq getNodePathsPartitionReq = (GetNodePathsPartitionReq) req;
    partialPath = getNodePathsPartitionReq.getPartialPath();
    level = getNodePathsPartitionReq.getLevel();
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
    allAttributes.add(clusterReceiverInfo);
    return allAttributes;
  }
}
