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
package org.apache.iotdb.db.service;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.UrlUtils;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DataNodeServerCommandLine extends ServerCommandLine {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeServerCommandLine.class);

  // join an established cluster
  public static final String MODE_START = "-s";
  // send a request to remove a node, more arguments: ip-of-removed-node
  // metaport-of-removed-node
  public static final String MODE_REMOVE = "-r";

  private static final String USAGE =
      "Usage: <-s|-r> "
          + "[-D{} <configure folder>] \n"
          + "-s: start the node to the cluster\n"
          + "-r: remove the node out of the cluster\n";

  @Override
  protected String getUsage() {
    return USAGE;
  }

  @Override
  protected int run(String[] args) throws Exception {
    if (args.length < 1) {
      usage(null);
      return -1;
    }

    DataNode dataNode = DataNode.getInstance();

    String mode = args[0];
    LOGGER.info("Running mode {}", mode);

    // Start IoTDB kernel first, then start the cluster module
    if (MODE_START.equals(mode)) {
      dataNode.doAddNode();
    } else if (MODE_REMOVE.equals(mode)) {
      doRemoveDataNode(args);
    } else {
      LOGGER.error("Unrecognized mode {}", mode);
    }
    return 0;
  }

  /**
   * remove datanodes from cluster
   *
   * @param args id or ip:rpc_port for removed datanode
   */
  private void doRemoveDataNode(String[] args)
      throws BadNodeUrlException, TException, IoTDBException, ClientManagerException {

    if (args.length != 2) {
      LOGGER.info("Usage: <node-id>/<ip>:<rpc-port>");
      return;
    }

    LOGGER.info("Starting to remove DataNode from cluster, parameter: {}, {}", args[0], args[1]);

    // Load ConfigNodeList from system.properties file
    ConfigNodeInfo.getInstance().loadConfigNodeList();

    List<TDataNodeLocation> dataNodeLocations = buildDataNodeLocations(args[1]);
    if (dataNodeLocations.isEmpty()) {
      throw new BadNodeUrlException("No DataNode to remove");
    }
    LOGGER.info("Start to remove datanode, removed datanode endpoints: {}", dataNodeLocations);
    TDataNodeRemoveReq removeReq = new TDataNodeRemoveReq(dataNodeLocations);
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TDataNodeRemoveResp removeResp = configNodeClient.removeDataNode(removeReq);
      LOGGER.info("Remove result {} ", removeResp);
      if (removeResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBException(
            removeResp.getStatus().toString(), removeResp.getStatus().getCode());
      }
      LOGGER.info(
          "Submit remove-datanode request successfully, but the process may fail. "
              + "more details are shown in the logs of confignode-leader and removed-datanode, "
              + "and after the process of removing datanode ends successfully, "
              + "you are supposed to delete directory and data of the removed-datanode manually");
    }
  }

  /**
   * fetch all datanode info from ConfigNode, then compare with input 'args'
   *
   * @param args datanode id or ip:rpc_port
   * @return TDataNodeLocation list
   */
  private List<TDataNodeLocation> buildDataNodeLocations(String args) {
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    if (args == null || args.trim().isEmpty()) {
      return dataNodeLocations;
    }

    // Now support only single datanode deletion
    if (args.split(",").length > 1) {
      LOGGER.info("Incorrect input format, usage: <id>/<ip>:<rpc-port>");
      return dataNodeLocations;
    }

    // Below supports multiple datanode deletion, split by ',', and is reserved for extension
    List<NodeCoordinate> endPoints = parseCoordinates(args);
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      dataNodeLocations =
          client.getDataNodeConfiguration(-1).getDataNodeConfigurationMap().values().stream()
              .map(TDataNodeConfiguration::getLocation)
              .filter(
                  location ->
                      endPoints.stream()
                          .anyMatch(nodeCoordinate -> nodeCoordinate.matches(location)))
              .collect(Collectors.toList());
    } catch (TException | ClientManagerException e) {
      LOGGER.error("Get data node locations failed", e);
    }

    return dataNodeLocations;
  }

  protected List<NodeCoordinate> parseCoordinates(String coordinatesString) {
    if (coordinatesString == null) {
      return Collections.emptyList();
    }

    // Multiple coordinates are separated by ","
    String[] coordinateStrings = coordinatesString.split(",");
    List<NodeCoordinate> coordinates = new ArrayList<>(coordinateStrings.length);
    for (String coordinate : coordinateStrings) {
      // If the string contains a ":" then this is an IP+Port configuration
      if (coordinate.contains(":")) {
        coordinates.add(new NodeCoordinateIP(UrlUtils.parseTEndPointIpv4AndIpv6Url(coordinate)));
      }
      // In the other case, we expect it to be a numeric value referring to the node-id
      else if (NumberUtils.isCreatable(coordinate)) {
        coordinates.add(new NodeCoordinateNodeId(Integer.parseInt(coordinate)));
      }
    }
    return coordinates;
  }

  protected interface NodeCoordinate {
    // Returns true if the given location matches this coordinate
    boolean matches(TDataNodeLocation location);
  }

  /** Implementation of a NodeCoordinate that uses ip and port to match. */
  protected static class NodeCoordinateIP implements NodeCoordinate {
    private final TEndPoint endPoint;

    public NodeCoordinateIP(TEndPoint endPoint) {
      this.endPoint = endPoint;
    }

    @Override
    public boolean matches(TDataNodeLocation location) {
      return endPoint.ip.equals(location.getClientRpcEndPoint().getIp())
          && endPoint.port == location.getClientRpcEndPoint().getPort();
    }
  }

  /** Implementation of a NodeCoordinate that uses the node id to match. */
  protected static class NodeCoordinateNodeId implements NodeCoordinate {
    private final int nodeId;

    public NodeCoordinateNodeId(int nodeId) {
      this.nodeId = nodeId;
    }

    @Override
    public boolean matches(TDataNodeLocation location) {
      return location.isSetDataNodeId() && location.dataNodeId == nodeId;
    }
  }
}
