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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Startup check utils before register/restart a ConfigNode/DataNode */
public class ClusterNodeStartUtils {

  private static final String CLUSTER_NAME =
      ConfigNodeDescriptor.getInstance().getConf().getClusterName();

  private static final String POSSIBLE_SOLUTIONS = " Possible solutions are as follows:\r\n";

  public static final TSStatus ACCEPT_NODE_REGISTRATION =
      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
          .setMessage("Accept Node registration.");
  public static final TSStatus ACCEPT_NODE_RESTART =
      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()).setMessage("Accept Node restart.");

  public static TSStatus confirmNodeRegistration(
      NodeType nodeType, String clusterName, Object nodeLocation, ConfigManager configManager) {

    final String CONF_FILE_NAME =
        NodeType.ConfigNode.equals(nodeType)
            ? ConfigNodeConstant.CONF_FILE_NAME
            : IoTDBConstant.DATA_NODE_CONF_FILE_NAME;
    TSStatus status = new TSStatus();

    /* Reject start if the cluster name is error */
    if (!CLUSTER_NAME.equals(clusterName)) {
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              "Reject %s start. Because the ClusterName of the current %s and the target cluster are inconsistent. "
                  + "ClusterName of the current Node: %s, ClusterName of the target cluster: %s."
                  + POSSIBLE_SOLUTIONS
                  + "\t1. Change the target_config_node_list parameter in %s to join the correct cluster."
                  + "\n\t2. Change the cluster_name parameter in %s to match the target cluster",
              nodeType.getNodeType(),
              nodeType.getNodeType(),
              clusterName,
              CLUSTER_NAME,
              CONF_FILE_NAME,
              CONF_FILE_NAME));
      return status;
    }

    /* Check if there exist conflict TEndPoints */
    List<TEndPoint> conflictEndPoints;
    switch (nodeType) {
      case ConfigNode:
        conflictEndPoints =
            checkConflictTEndPointForNewConfigNode(
                (TConfigNodeLocation) nodeLocation,
                configManager.getNodeManager().getRegisteredConfigNodes());
        break;
      case DataNode:
      default:
        conflictEndPoints =
            checkConflictTEndPointForNewDataNode(
                (TDataNodeLocation) nodeLocation,
                configManager.getNodeManager().getRegisteredDataNodes());
        break;
    }

    if (!conflictEndPoints.isEmpty()) {
      /* Reject Node registration because there exist conflict TEndPoints */
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              "Reject %s registration. Because the following ip:port: %s of the current %s is conflicted with other registered Nodes in the cluster."
                  + POSSIBLE_SOLUTIONS
                  + "\t1. Use SQL: \"show cluster details\" to find out the conflict Nodes. Remove them and retry start."
                  + "\n\t2. Change the conflict ip:port configurations in %s file and retry start.",
              nodeType.getNodeType(),
              conflictEndPoints,
              nodeType.getNodeType(),
              CONF_FILE_NAME));
      return status;
    } else {
      /* Accept registration if all TEndPoints aren't conflict */
      return ACCEPT_NODE_REGISTRATION;
    }
  }

  public static TSStatus confirmNodeRestart(
      NodeType nodeType,
      String clusterName,
      int nodeId,
      Object nodeLocation,
      ConfigManager configManager) {

    final String CONF_FILE_NAME =
        NodeType.ConfigNode.equals(nodeType)
            ? ConfigNodeConstant.CONF_FILE_NAME
            : IoTDBConstant.DATA_NODE_CONF_FILE_NAME;
    TSStatus status = new TSStatus();

    /* Reject restart if the cluster name is error */
    if (!CLUSTER_NAME.equals(clusterName)) {
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              "Reject %s restart. Because the ClusterName of the current %s and the target cluster are inconsistent. "
                  + "ClusterName of the current Node: %s, ClusterName of the target cluster: %s."
                  + POSSIBLE_SOLUTIONS
                  + "\t1. Change the target_config_node_list parameter in %s to join the correct cluster."
                  + "\n\t2. Change the cluster_name parameter in %s to match the target cluster",
              nodeType.getNodeType(),
              nodeType.getNodeType(),
              clusterName,
              CLUSTER_NAME,
              CONF_FILE_NAME,
              CONF_FILE_NAME));
      return status;
    }

    /* Reject restart if the nodeId is error */
    if (nodeId < 0) {
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              "Reject %s restart. Because the nodeId of the current %s is %d."
                  + POSSIBLE_SOLUTIONS
                  + "\t1. Delete \"data\" dir and retry.",
              nodeType.getNodeType(),
              nodeType.getNodeType(),
              nodeId));
      return status;
    }

    Object matchedNodeLocation;
    switch (nodeType) {
      case ConfigNode:
        matchedNodeLocation =
            matchRegisteredConfigNode(
                (TConfigNodeLocation) nodeLocation,
                configManager.getNodeManager().getRegisteredConfigNodes());
        break;
      case DataNode:
      default:
        matchedNodeLocation =
            matchRegisteredDataNode(
                (TDataNodeLocation) nodeLocation,
                configManager.getNodeManager().getRegisteredDataNodes());
        break;
    }

    /* Reject restart because there are no corresponding Node in the cluster */
    if (matchedNodeLocation == null) {
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              "Reject %s restart. Because there are no corresponding %s(whose nodeId=%d) in the cluster."
                  + POSSIBLE_SOLUTIONS
                  + "\t1. Maybe you've already removed the current %s(whose nodeId=%d). Please delete the useless 'data' dir and retry start.",
              nodeType.getNodeType(),
              nodeType.getNodeType(),
              nodeId,
              nodeType.getNodeType(),
              nodeId));
      return status;
    }

    boolean acceptRestart = true;
    Set<Integer> updatedTEndPoints;
    switch (nodeType) {
      case ConfigNode:
        updatedTEndPoints =
            checkUpdatedTEndPointOfConfigNode(
                (TConfigNodeLocation) nodeLocation, (TConfigNodeLocation) matchedNodeLocation);
        if (!updatedTEndPoints.isEmpty()) {
          // TODO: Accept internal TEndPoints
          acceptRestart = false;
        }
        break;
      case DataNode:
      default:
        updatedTEndPoints =
            checkUpdatedTEndPointOfDataNode(
                (TDataNodeLocation) nodeLocation, (TDataNodeLocation) matchedNodeLocation);
        if (updatedTEndPoints.stream().max(Integer::compare).orElse(-1) > 0) {
          // TODO: Accept internal TEndPoints
          acceptRestart = false;
        }
        break;
    }

    if (!acceptRestart) {
      /* Reject restart because some internal TEndPoints have been changed */
      status.setCode(TSStatusCode.REJECT_NODE_START.getStatusCode());
      status.setMessage(
          String.format(
              "Reject %s restart. Because the internal TEndPoints of this %s can't be modified."
                  + POSSIBLE_SOLUTIONS
                  + "\t1. Please keep the internal TEndPoints of this Node the same as before.",
              nodeType.getNodeType(),
              nodeType.getNodeType()));
      return status;
    } else {
      /* Accept Node restart */
      return ACCEPT_NODE_RESTART;
    }
  }

  /**
   * Check if there exist conflict TEndPoints on the ConfigNode to be registered
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
   * Check if there exist conflict TEndPoints on the DataNode to be registered
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
          .getClientRpcEndPoint()
          .equals(newDataNodeLocation.getClientRpcEndPoint())) {
        conflictEndPointSet.add(newDataNodeLocation.getClientRpcEndPoint());
      }
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
