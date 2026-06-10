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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Startup check utils before register/restart a ConfigNode/DataNode. */
public class ClusterNodeStartUtils {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private static final String POSSIBLE_SOLUTIONS = " Possible solutions are as follows:\r\n";

  public static final TSStatus ACCEPT_NODE_REGISTRATION =
      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
          .setMessage(ManagerMessages.MESSAGE_ACCEPT_NODE_REGISTRATION_4133276A);
  public static final TSStatus ACCEPT_NODE_RESTART =
      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
          .setMessage(ManagerMessages.MESSAGE_ACCEPT_NODE_RESTART_1BC1A8DD);

  private ClusterNodeStartUtils() {
    // Empty constructor
  }

  private static TSStatus confirmClusterName(NodeType nodeType, String clusterName) {
    TSStatus status = new TSStatus();
    if (!CONF.getClusterName().equals(clusterName)) {
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              ManagerMessages
                      .MESSAGE_REJECT_ARG_START_BECAUSE_CLUSTERNAME_CURRENT_ARG_TARGET_CLUSTER_INCONSISTENT_B9E197DB
                  + ManagerMessages
                      .MESSAGE_CLUSTERNAME_CURRENT_NODE_ARG_CLUSTERNAME_TARGET_CLUSTER_ARG_5C34BE8D
                  + POSSIBLE_SOLUTIONS
                  + ManagerMessages
                      .MESSAGE_1_CHANGE_SEED_CONFIG_NODE_PARAMETER_ARG_JOIN_CORRECT_CLUSTER_5E9D753C
                  + ManagerMessages
                      .MESSAGE_2_CHANGE_CLUSTER_NAME_PARAMETER_ARG_MATCH_TARGET_CLUSTER_0A0DB235,
              nodeType.getNodeType(),
              nodeType.getNodeType(),
              clusterName,
              CONF.getClusterName(),
              CommonConfig.SYSTEM_CONFIG_NAME,
              CommonConfig.SYSTEM_CONFIG_NAME));
      return status;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static TSStatus rejectRegistrationBecauseConflictEndPoints(
      NodeType nodeType, List<TEndPoint> conflictEndPoints) {
    TSStatus status = new TSStatus();
    status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
    status.setMessage(
        String.format(
            ManagerMessages
                    .MESSAGE_REJECT_ARG_REGISTRATION_BECAUSE_FOLLOWING_IP_PORT_ARG_CURRENT_ARG_CB78CC3B
                + POSSIBLE_SOLUTIONS
                + ManagerMessages
                    .MESSAGE_1_USE_SQL_SHOW_CLUSTER_DETAILS_FIND_OUT_CONFLICT_NODES_A1195AEA
                + ManagerMessages
                    .MESSAGE_2_CHANGE_CONFLICT_IP_PORT_CONFIGURATIONS_ARG_FILE_RETRY_START_CF3F08F6,
            nodeType.getNodeType(),
            conflictEndPoints,
            nodeType.getNodeType(),
            CommonConfig.SYSTEM_CONFIG_NAME));
    return status;
  }

  public static TSStatus confirmClusterId(ConfigManager configManager) {
    TSStatus status = new TSStatus();
    final String clusterId =
        configManager
            .getClusterManager()
            .getClusterIdWithRetry(
                CommonDescriptor.getInstance().getConfig().getCnConnectionTimeoutInMS() / 2);
    if (clusterId == null) {
      status
          .setCode(TSStatusCode.GET_CLUSTER_ID_ERROR.getStatusCode())
          .setMessage(
              ManagerMessages.MESSAGE_CLUSTER_ID_HAS_NOT_GENERATED_PLEASE_TRY_AGAIN_LATER_58A1C3F2);
      return status;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public static TSStatus confirmDataNodeRegistration(
      TDataNodeRegisterReq req, ConfigManager configManager) {
    // Confirm cluster name
    TSStatus status = confirmClusterName(NodeType.DataNode, req.getClusterName());
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    // Confirm end point conflicts
    List<TEndPoint> conflictEndPoints =
        checkConflictTEndPointForNewDataNode(
            req.getDataNodeConfiguration().getLocation(),
            configManager.getNodeManager().getRegisteredDataNodes());
    if (!conflictEndPoints.isEmpty()) {
      return rejectRegistrationBecauseConflictEndPoints(NodeType.DataNode, conflictEndPoints);
    }
    // Confirm whether cluster id has been generated
    status = confirmClusterId(configManager);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    // Success
    return ACCEPT_NODE_REGISTRATION;
  }

  public static TSStatus confirmConfigNodeRegistration(
      TConfigNodeRegisterReq req, ConfigManager configManager) {
    // Confirm cluster name
    TSStatus status =
        confirmClusterName(NodeType.ConfigNode, req.getClusterParameters().getClusterName());
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    // Confirm end point conflicts
    List<TEndPoint> conflictEndPoints =
        checkConflictTEndPointForNewConfigNode(
            req.getConfigNodeLocation(), configManager.getNodeManager().getRegisteredConfigNodes());
    if (!conflictEndPoints.isEmpty()) {
      return rejectRegistrationBecauseConflictEndPoints(NodeType.ConfigNode, conflictEndPoints);
    }
    // Confirm whether cluster id has been generated
    status = confirmClusterId(configManager);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    // Success
    return ACCEPT_NODE_REGISTRATION;
  }

  public static TSStatus confirmAINodeRegistration(
      TAINodeRegisterReq req, ConfigManager configManager) {
    // Confirm cluster name
    TSStatus status = confirmClusterName(NodeType.AINode, req.getClusterName());
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    // Confirm end point conflicts
    List<TEndPoint> conflictEndPoints =
        checkConflictTEndPointForNewAINode(
            req.getAiNodeConfiguration().getLocation(),
            configManager.getNodeManager().getRegisteredAINodes());
    if (!conflictEndPoints.isEmpty()) {
      return rejectRegistrationBecauseConflictEndPoints(NodeType.AINode, conflictEndPoints);
    }
    // Confirm whether cluster id has been generated
    status = confirmClusterId(configManager);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    // Success
    return ACCEPT_NODE_REGISTRATION;
  }

  /**
   * Check if there exist conflict TEndPoints on the DataNode to be registered.
   *
   * @param newAINodeLocation The TDataNodeLocation of the DataNode to be registered
   * @param registeredAINodes All registered DataNodes
   * @return The conflict TEndPoints if exist
   */
  public static List<TEndPoint> checkConflictTEndPointForNewAINode(
      TAINodeLocation newAINodeLocation, List<TAINodeConfiguration> registeredAINodes) {
    Set<TEndPoint> conflictEndPointSet = new HashSet<>();
    for (TAINodeConfiguration registeredAINode : registeredAINodes) {
      TAINodeLocation registeredLocation = registeredAINode.getLocation();
      if (registeredLocation
          .getInternalEndPoint()
          .equals(newAINodeLocation.getInternalEndPoint())) {
        conflictEndPointSet.add(newAINodeLocation.getInternalEndPoint());
      }
    }

    return new ArrayList<>(conflictEndPointSet);
  }

  public static TSStatus confirmNodeRestart(
      NodeType nodeType,
      String clusterName,
      String clusterId,
      int nodeId,
      Object nodeLocation,
      ConfigManager configManager) {
    TSStatus status = new TSStatus();

    /* Reject restart if the cluster name is error */
    if (!CONF.getClusterName().equals(clusterName)) {
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              ManagerMessages
                      .MESSAGE_REJECT_ARG_RESTART_BECAUSE_CLUSTERNAME_CURRENT_ARG_TARGET_CLUSTER_INCONSISTENT_2075F29D
                  + ManagerMessages
                      .MESSAGE_CLUSTERNAME_CURRENT_NODE_ARG_CLUSTERNAME_TARGET_CLUSTER_ARG_5C34BE8D
                  + POSSIBLE_SOLUTIONS
                  + ManagerMessages
                      .MESSAGE_1_CHANGE_SEED_CONFIG_NODE_PARAMETER_ARG_JOIN_CORRECT_CLUSTER_5E9D753C
                  + ManagerMessages
                      .MESSAGE_2_CHANGE_CLUSTER_NAME_PARAMETER_ARG_MATCH_TARGET_CLUSTER_0A0DB235,
              nodeType.getNodeType(),
              nodeType.getNodeType(),
              clusterName,
              CONF.getClusterName(),
              CommonConfig.SYSTEM_CONFIG_NAME,
              CommonConfig.SYSTEM_CONFIG_NAME));
      return status;
    }

    /* Reject restart if the nodeId is error */
    if (nodeId < 0) {
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              ManagerMessages.MESSAGE_REJECT_ARG_RESTART_BECAUSE_NODEID_CURRENT_ARG_ARG_AC13EDD5
                  + POSSIBLE_SOLUTIONS
                  + ManagerMessages.MESSAGE_1_DELETE_DATA_DIR_RETRY_86A23473,
              nodeType.getNodeType(),
              nodeType.getNodeType(),
              nodeId));
      return status;
    }

    Object matchedNodeLocation = null;
    switch (nodeType) {
      case ConfigNode:
        if (nodeLocation instanceof TConfigNodeLocation) {
          matchedNodeLocation =
              matchRegisteredConfigNode(
                  (TConfigNodeLocation) nodeLocation,
                  configManager.getNodeManager().getRegisteredConfigNodes());
        }
        break;
      case AINode:
        if (nodeLocation instanceof TAINodeLocation) {
          matchedNodeLocation =
              matchRegisteredAINode(
                  (TAINodeLocation) nodeLocation,
                  configManager.getNodeManager().getRegisteredAINodes());
        }
        break;
      case DataNode:
      default:
        if (nodeLocation instanceof TDataNodeLocation) {
          matchedNodeLocation =
              matchRegisteredDataNode(
                  (TDataNodeLocation) nodeLocation,
                  configManager.getNodeManager().getRegisteredDataNodes());
        }
        break;
    }

    /* Reject restart because there are no corresponding Node in the cluster */
    if (matchedNodeLocation == null) {
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              ManagerMessages
                      .MESSAGE_REJECT_ARG_RESTART_BECAUSE_THERE_NO_CORRESPONDING_ARG_WHOSE_NODEID_455578E9
                  + POSSIBLE_SOLUTIONS
                  + ManagerMessages
                      .MESSAGE_1_MAYBE_YOU_VE_ALREADY_REMOVED_CURRENT_ARG_WHOSE_NODEID_92165504,
              nodeType.getNodeType(),
              nodeType.getNodeType(),
              nodeId,
              nodeType.getNodeType(),
              nodeId));
      return status;
    }

    boolean acceptRestart = true;
    Set<Integer> updatedTEndPoints = null;
    switch (nodeType) {
      case ConfigNode:
        if (nodeLocation instanceof TConfigNodeLocation) {
          updatedTEndPoints =
              checkUpdatedTEndPointOfConfigNode(
                  (TConfigNodeLocation) nodeLocation, (TConfigNodeLocation) matchedNodeLocation);
          if (!updatedTEndPoints.isEmpty()) {
            // TODO: Accept internal TEndPoints
            acceptRestart = false;
          }
        }
        break;
      case DataNode:
      default:
        if (nodeLocation instanceof TDataNodeLocation) {
          updatedTEndPoints =
              checkUpdatedTEndPointOfDataNode(
                  (TDataNodeLocation) nodeLocation, (TDataNodeLocation) matchedNodeLocation);
          if (updatedTEndPoints.stream().max(Integer::compare).orElse(-1) > 0) {
            // TODO: Accept internal TEndPoints
            acceptRestart = false;
          }
        }
        break;
    }

    // check clusterId if not empty
    if (clusterId != null
        && !clusterId.isEmpty()
        && !clusterId.equals(configManager.getClusterManager().getClusterId())) {
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              ManagerMessages
                      .MESSAGE_REJECT_ARG_RESTART_BECAUSE_CLUSTERID_CURRENT_ARG_TARGET_CLUSTER_INCONSISTENT_0398A6CE
                  + ManagerMessages
                      .MESSAGE_CLUSTERID_CURRENT_NODE_ARG_CLUSTERID_TARGET_CLUSTER_ARG_23C42434
                  + POSSIBLE_SOLUTIONS
                  + ManagerMessages
                      .MESSAGE_1_PLEASE_CHECK_IF_NODE_CONFIGURATION_PATH_CORRECT_7FB5D559,
              nodeType.getNodeType(),
              nodeType.getNodeType(),
              clusterId,
              configManager.getClusterManager().getClusterId()));
      return status;
    }

    if (!acceptRestart) {
      /* Reject restart because some internal TEndPoints have been changed */
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              ManagerMessages
                      .MESSAGE_REJECT_ARG_RESTART_BECAUSE_INTERNAL_TENDPOINTS_ARG_CAN_T_MODIFIED_A58B99F0
                  + POSSIBLE_SOLUTIONS
                  + ManagerMessages
                      .MESSAGE_1_PLEASE_KEEP_INTERNAL_TENDPOINTS_NODE_SAME_AS_BEFORE_2FDB2034,
              nodeType.getNodeType(),
              nodeType.getNodeType()));
      return status;
    } else {
      /* Accept Node restart */
      return ACCEPT_NODE_RESTART;
    }
  }

  /**
   * Check if there exist conflict TEndPoints on the ConfigNode to be registered.
   *
   * @param newConfigNodeLocation The TConfigNode of the ConfigNode to be registered
   * @param registeredConfigNodes All registered ConfigNodes
   * @return The conflict TEndPoints if exist
   */
  public static List<TEndPoint> checkConflictTEndPointForNewConfigNode(
      TConfigNodeLocation newConfigNodeLocation, List<TConfigNodeLocation> registeredConfigNodes) {
    Set<TEndPoint> conflictEndPointSet = new HashSet<>();
    for (TConfigNodeLocation registeredConfigNode : registeredConfigNodes) {
      if (registeredConfigNode
          .getInternalEndPoint()
          .equals(newConfigNodeLocation.getInternalEndPoint())) {
        conflictEndPointSet.add(newConfigNodeLocation.getInternalEndPoint());
      }
      if (registeredConfigNode
          .getConsensusEndPoint()
          .equals(newConfigNodeLocation.getConsensusEndPoint())) {
        conflictEndPointSet.add(newConfigNodeLocation.getConsensusEndPoint());
      }
    }

    return new ArrayList<>(conflictEndPointSet);
  }

  /**
   * Check if there exist conflict TEndPoints on the DataNode to be registered.
   *
   * @param newDataNodeLocation The TDataNodeLocation of the DataNode to be registered
   * @param registeredDataNodes All registered DataNodes
   * @return The conflict TEndPoints if exist
   */
  public static List<TEndPoint> checkConflictTEndPointForNewDataNode(
      TDataNodeLocation newDataNodeLocation, List<TDataNodeConfiguration> registeredDataNodes) {
    Set<TEndPoint> conflictEndPointSet = new HashSet<>();
    for (TDataNodeConfiguration registeredDataNode : registeredDataNodes) {
      TDataNodeLocation registeredLocation = registeredDataNode.getLocation();

      if (registeredLocation
          .getInternalEndPoint()
          .equals(newDataNodeLocation.getInternalEndPoint())) {
        conflictEndPointSet.add(newDataNodeLocation.getInternalEndPoint());
      }
      if (registeredLocation
          .getMPPDataExchangeEndPoint()
          .equals(newDataNodeLocation.getMPPDataExchangeEndPoint())) {
        conflictEndPointSet.add(newDataNodeLocation.getMPPDataExchangeEndPoint());
      }
      if (registeredLocation
          .getSchemaRegionConsensusEndPoint()
          .equals(newDataNodeLocation.getSchemaRegionConsensusEndPoint())) {
        conflictEndPointSet.add(newDataNodeLocation.getSchemaRegionConsensusEndPoint());
      }
      if (registeredLocation
          .getDataRegionConsensusEndPoint()
          .equals(newDataNodeLocation.getDataRegionConsensusEndPoint())) {
        conflictEndPointSet.add(newDataNodeLocation.getDataRegionConsensusEndPoint());
      }
    }

    return new ArrayList<>(conflictEndPointSet);
  }

  /**
   * Check if there exists a registered ConfigNode who has the same index of the given one.
   *
   * @param configNodeLocation The given ConfigNode
   * @param registeredConfigNodes Registered ConfigNodes
   * @return The ConfigNodeLocation who has the same index of the given one, null otherwise.
   */
  public static TConfigNodeLocation matchRegisteredConfigNode(
      TConfigNodeLocation configNodeLocation, List<TConfigNodeLocation> registeredConfigNodes) {
    for (TConfigNodeLocation registeredConfigNode : registeredConfigNodes) {
      if (registeredConfigNode.getConfigNodeId() == configNodeLocation.getConfigNodeId()) {
        return registeredConfigNode;
      }
    }

    return null;
  }

  /**
   * Check if there exists a registered AINode who has the same index of the given one.
   *
   * @param aiNodeLocation The given AINode
   * @param registeredAINodes Registered AINodes
   * @return The AINodeLocation who has the same index of the given one, null otherwise.
   */
  public static TAINodeLocation matchRegisteredAINode(
      TAINodeLocation aiNodeLocation, List<TAINodeConfiguration> registeredAINodes) {
    for (TAINodeConfiguration registeredAINode : registeredAINodes) {
      if (registeredAINode.getLocation().getAiNodeId() == aiNodeLocation.getAiNodeId()) {
        return registeredAINode.getLocation();
      }
    }

    return null;
  }

  /**
   * Check if there exists a registered DataNode who has the same index of the given one.
   *
   * @param dataNodeLocation The given DataNode
   * @param registeredDataNodes Registered DataNodes
   * @return The DataNodeLocation who has the same index of the given one, null otherwise.
   */
  public static TDataNodeLocation matchRegisteredDataNode(
      TDataNodeLocation dataNodeLocation, List<TDataNodeConfiguration> registeredDataNodes) {
    for (TDataNodeConfiguration registeredDataNode : registeredDataNodes) {
      if (registeredDataNode.getLocation().getDataNodeId() == dataNodeLocation.getDataNodeId()) {
        return registeredDataNode.getLocation();
      }
    }

    return null;
  }

  /**
   * Check if some TEndPoints of the specified ConfigNode have updated.
   *
   * @param restartLocation The location of restart ConfigNode
   * @param recordLocation The record ConfigNode location
   * @return The set of TEndPoints that have modified.
   */
  public static Set<Integer> checkUpdatedTEndPointOfConfigNode(
      TConfigNodeLocation restartLocation, TConfigNodeLocation recordLocation) {
    Set<Integer> updatedTEndPoints = new HashSet<>();
    if (!recordLocation.getInternalEndPoint().equals(restartLocation.getInternalEndPoint())) {
      updatedTEndPoints.add(0);
    }
    if (!recordLocation.getConsensusEndPoint().equals(restartLocation.getConsensusEndPoint())) {
      updatedTEndPoints.add(1);
    }
    return updatedTEndPoints;
  }

  /**
   * Check if some TEndPoints of the specified DataNode have updated.
   *
   * @param restartLocation The location of restart DataNode
   * @param recordLocation The record DataNode location
   * @return The set of TEndPoints that have modified.
   */
  public static Set<Integer> checkUpdatedTEndPointOfDataNode(
      TDataNodeLocation restartLocation, TDataNodeLocation recordLocation) {
    Set<Integer> updatedTEndPoints = new HashSet<>();
    if (!recordLocation.getClientRpcEndPoint().equals(restartLocation.getClientRpcEndPoint())) {
      updatedTEndPoints.add(0);
    }
    if (!recordLocation.getInternalEndPoint().equals(restartLocation.getInternalEndPoint())) {
      updatedTEndPoints.add(1);
    }
    if (!recordLocation
        .getMPPDataExchangeEndPoint()
        .equals(restartLocation.getMPPDataExchangeEndPoint())) {
      updatedTEndPoints.add(2);
    }
    if (!recordLocation
        .getSchemaRegionConsensusEndPoint()
        .equals(restartLocation.getSchemaRegionConsensusEndPoint())) {
      updatedTEndPoints.add(3);
    }
    if (!recordLocation
        .getDataRegionConsensusEndPoint()
        .equals(restartLocation.getDataRegionConsensusEndPoint())) {
      updatedTEndPoints.add(4);
    }
    return updatedTEndPoints;
  }
}
