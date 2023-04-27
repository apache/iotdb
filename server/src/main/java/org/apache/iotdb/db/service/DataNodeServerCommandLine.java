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
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNumeric;

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
    try {
      List<TEndPoint> endPoints = NodeUrlUtils.parseTEndPointUrls(args);
      try (ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        dataNodeLocations =
            client.getDataNodeConfiguration(-1).getDataNodeConfigurationMap().values().stream()
                .map(TDataNodeConfiguration::getLocation)
                .filter(location -> endPoints.contains(location.getClientRpcEndPoint()))
                .collect(Collectors.toList());
      } catch (TException | ClientManagerException e) {
        LOGGER.error("Get data node locations failed", e);
      }
    } catch (BadNodeUrlException e) {
      try (ConfigNodeClient client =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        for (String id : args.split(",")) {
          if (!isNumeric(id)) {
            LOGGER.warn("Incorrect id format {}, skipped...", id);
            continue;
          }
          List<TDataNodeLocation> nodeLocationResult =
              client.getDataNodeConfiguration(Integer.parseInt(id)).getDataNodeConfigurationMap()
                  .values().stream()
                  .map(TDataNodeConfiguration::getLocation)
                  .collect(Collectors.toList());
          if (nodeLocationResult.isEmpty()) {
            LOGGER.warn("DataNode {} is not in cluster, skipped...", id);
            continue;
          }
          if (!dataNodeLocations.contains(nodeLocationResult.get(0))) {
            dataNodeLocations.add(nodeLocationResult.get(0));
          }
        }
      } catch (TException | ClientManagerException e1) {
        LOGGER.error("Get data node locations failed", e);
      }
    }
    return dataNodeLocations;
  }
}
