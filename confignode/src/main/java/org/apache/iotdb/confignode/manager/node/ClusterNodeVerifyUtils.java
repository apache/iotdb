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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClusterNodeVerifyUtils {

  /**
   * Check if there exist conflict TEndPoints on the ConfigNode to be registered
   *
   * @param newConfigNodeLocation The TConfigNode of the ConfigNode to be registered
   * @param registeredConfigNodes The registered ConfigNodes
   * @param conflictEndPoints Put the conflict TEndPoints if exist
   * @return True if there are no any conflict TEndPoint. False otherwise
   */
  public static boolean checkConflictTEndPointForNewConfigNode(
      TConfigNodeLocation newConfigNodeLocation,
      List<TConfigNodeLocation> registeredConfigNodes,
      List<TEndPoint> conflictEndPoints) {
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

    if (conflictEndPoints.isEmpty()) {
      return true;
    } else {
      conflictEndPoints.addAll(conflictEndPointSet);
      return false;
    }
  }

  public static TConfigNodeLocation matchRegisteredConfigNode(
      TConfigNodeLocation configNodeLocation, List<TConfigNodeLocation> registeredConfigNodes) {
    for (TConfigNodeLocation registeredConfigNode : registeredConfigNodes) {
      if (registeredConfigNode.getConfigNodeId() == configNodeLocation.getConfigNodeId()) {
        return registeredConfigNode;
      }
    }

    return null;
  }

  public static boolean compareTEndPointsOfTConfigNodeLocation(
      TConfigNodeLocation configNodeLocationA, TConfigNodeLocation configNodeLocationB) {
    if (!configNodeLocationA
        .getInternalEndPoint()
        .equals(configNodeLocationB.getInternalEndPoint())) {
      return false;
    }
    return configNodeLocationA
        .getConsensusEndPoint()
        .equals(configNodeLocationB.getConsensusEndPoint());
  }

  /**
   * Check if there exist conflict TEndPoints on the DataNode to be registered
   *
   * @param newDataNodeLocation The TDataNodeLocation of the DataNode to be registered
   * @param registeredDataNodes The registered DataNodes
   * @param conflictEndPoints Put the conflict TEndPoints if exist
   * @return True if there are no any conflict TEndPoint. False otherwise
   */
  public static boolean checkConflictTEndPointForNewDataNode(
      TDataNodeLocation newDataNodeLocation,
      List<TDataNodeConfiguration> registeredDataNodes,
      List<TEndPoint> conflictEndPoints) {
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

    if (conflictEndPoints.isEmpty()) {
      return true;
    } else {
      conflictEndPoints.addAll(conflictEndPointSet);
      return false;
    }
  }

  public static TDataNodeLocation matchRegisteredDataNode(
      TDataNodeLocation dataNodeLocation, List<TDataNodeConfiguration> registeredDataNodes) {
    for (TDataNodeConfiguration registeredDataNode : registeredDataNodes) {
      if (registeredDataNode.getLocation().getDataNodeId() == dataNodeLocation.getDataNodeId()) {
        return registeredDataNode.getLocation();
      }
    }

    return null;
  }

  public static boolean compareTEndPointsOfTDataNodeLocation(
      TDataNodeLocation dataNodeLocationA, TDataNodeLocation dataNodeLocationB) {
    if (!dataNodeLocationA
        .getClientRpcEndPoint()
        .equals(dataNodeLocationB.getClientRpcEndPoint())) {
      return false;
    }
    if (!dataNodeLocationA.getInternalEndPoint().equals(dataNodeLocationB.getInternalEndPoint())) {
      return false;
    }
    if (!dataNodeLocationA
        .getMPPDataExchangeEndPoint()
        .equals(dataNodeLocationB.getMPPDataExchangeEndPoint())) {
      return false;
    }
    if (!dataNodeLocationA
        .getSchemaRegionConsensusEndPoint()
        .equals(dataNodeLocationB.getSchemaRegionConsensusEndPoint())) {
      return false;
    }
    return dataNodeLocationA
        .getDataRegionConsensusEndPoint()
        .equals(dataNodeLocationB.getDataRegionConsensusEndPoint());
  }
}
