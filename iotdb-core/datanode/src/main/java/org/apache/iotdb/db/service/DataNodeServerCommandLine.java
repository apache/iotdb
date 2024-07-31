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
import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataNodeServerCommandLine extends ServerCommandLine {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeServerCommandLine.class);

  // join an established cluster
  public static final String MODE_START = "-s";
  // send a request to remove a node, more arguments: ip-of-removed-node
  // metaport-of-removed-node
  public static final String MODE_REMOVE = "-r";

  private final ConfigNodeInfo configNodeInfo;
  private final IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager;
  private final DataNode dataNode;

  private static final String USAGE =
      "Usage: <-s|-r> "
          + "[-D{} <configure folder>] \n"
          + "-s: start the node to the cluster\n"
          + "-r: remove the node out of the cluster\n";

  /** Default constructor using the singletons for initializing the relationship. */
  public DataNodeServerCommandLine() {
    configNodeInfo = ConfigNodeInfo.getInstance();
    configNodeClientManager = ConfigNodeClientManager.getInstance();
    dataNode = DataNode.getInstance();
  }

  /**
   * Additional constructor allowing injection of custom instances (mainly for testing)
   *
   * @param configNodeInfo config node info
   * @param configNodeClientManager config node client manager
   * @param dataNode data node
   */
  public DataNodeServerCommandLine(
      ConfigNodeInfo configNodeInfo,
      IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager,
      DataNode dataNode) {
    this.configNodeInfo = configNodeInfo;
    this.configNodeClientManager = configNodeClientManager;
    this.dataNode = dataNode;
  }

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

    String mode = args[0];
    LOGGER.info("Running mode {}", mode);

    // Start IoTDB kernel first, then start the cluster module
    if (MODE_START.equals(mode)) {
      dataNode.doAddNode();
    } else if (MODE_REMOVE.equals(mode)) {
      return doRemoveDataNode(args);
    } else {
      LOGGER.error("Unrecognized mode {}", mode);
    }
    return 0;
  }

  /**
   * remove data-nodes from cluster
   *
   * @param args id or ip:rpc_port for removed datanode
   */
  private int doRemoveDataNode(String[] args)
      throws BadNodeUrlException, TException, IoTDBException, ClientManagerException {

    if (args.length != 2) {
      LOGGER.info("Usage: <node-id>/<ip>:<rpc-port>");
      return -1;
    }

    // REMARK: Don't need null or empty-checks for args[0] or args[1], as if they were
    // empty, the JVM would have not received them.

    LOGGER.info("Starting to remove DataNode from cluster, parameter: {}, {}", args[0], args[1]);

    // Load ConfigNodeList from system.properties file
    configNodeInfo.loadConfigNodeList();

    List<TDataNodeLocation> dataNodeLocations = buildDataNodeLocations(args[1]);
    if (dataNodeLocations.isEmpty()) {
      throw new BadNodeUrlException("No DataNode to remove");
    }
    LOGGER.info("Start to remove datanode, removed datanode endpoints: {}", dataNodeLocations);
    TDataNodeRemoveReq removeReq = new TDataNodeRemoveReq(dataNodeLocations);
    try (ConfigNodeClient configNodeClient =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
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
    return 0;
  }

  /**
   * fetch all datanode info from ConfigNode, then compare with input 'args'
   *
   * @param args datanode id or ip:rpc_port
   * @return TDataNodeLocation list
   */
  private List<TDataNodeLocation> buildDataNodeLocations(String args) {
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();

    // Now support only single datanode deletion
    if (args.split(",").length > 1) {
      throw new IllegalArgumentException("Currently only removing single nodes is supported.");
    }

    // Below supports multiple datanode deletion, split by ',', and is reserved for extension
    List<NodeCoordinate> nodeCoordinates = parseCoordinates(args);
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      dataNodeLocations =
          client.getDataNodeConfiguration(-1).getDataNodeConfigurationMap().values().stream()
              .map(TDataNodeConfiguration::getLocation)
              .filter(
                  location ->
                      nodeCoordinates.stream()
                          .anyMatch(nodeCoordinate -> nodeCoordinate.matches(location)))
              .collect(Collectors.toList());
    } catch (TException | ClientManagerException e) {
      LOGGER.error("Get data node locations failed", e);
    }

    return dataNodeLocations;
  }

  protected List<NodeCoordinate> parseCoordinates(String coordinatesString) {
    // Multiple nodeIds are separated by ","
    String[] nodeIdStrings = coordinatesString.split(",");
    List<NodeCoordinate> nodeIdCoordinates = new ArrayList<>(nodeIdStrings.length);
    for (String nodeId : nodeIdStrings) {
      // In the other case, we expect it to be a numeric value referring to the node-id
      if (NumberUtils.isCreatable(nodeId)) {
        nodeIdCoordinates.add(new NodeCoordinateNodeId(Integer.parseInt(nodeId)));
      } else {
        LOGGER.error("Invalid format. Expected a numeric node id, but got: {}", nodeId);
      }
    }
    return nodeIdCoordinates;
  }

  protected interface NodeCoordinate {
    // Returns true if the given location matches this coordinate
    boolean matches(TDataNodeLocation location);
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
