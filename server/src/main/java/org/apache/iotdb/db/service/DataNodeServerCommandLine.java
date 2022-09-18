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
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.IoTDBStopCheck;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataNodeServerCommandLine extends ServerCommandLine {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeServerCommandLine.class);

  // join an established cluster
  private static final String MODE_START = "-s";
  // send a request to remove a node, more arguments: ip-of-removed-node
  // metaport-of-removed-node
  private static final String MODE_REMOVE = "-r";

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
    // check config of iotdb,and set some configs in cluster mode
    try {
      dataNode.serverCheckAndInit();
    } catch (ConfigurationException | IOException e) {
      logger.error("meet error when doing start checking", e);
      return -1;
    }
    String mode = args[0];
    logger.info("Running mode {}", mode);

    // initialize the current node and its services
    if (!dataNode.initLocalEngines()) {
      logger.error("initLocalEngines error, stop process!");
      return -1;
    }

    // we start IoTDB kernel first. then we start the cluster module.
    if (MODE_START.equals(mode)) {
      dataNode.doAddNode();
    } else if (MODE_REMOVE.equals(mode)) {
      doRemoveNode(args);
    } else {
      logger.error("Unrecognized mode {}", mode);
    }
    return 0;
  }

  /**
   * remove datanodes from cluster
   *
   * @param args IPs for removed datanodes, split with ','
   */
  private void doRemoveNode(String[] args) throws Exception {
    // throw all exception to ServerCommandLine, it used System.exit
    removePrepare(args);
    removeNodesFromCluster(args);
    removeTail();
  }

  private void removePrepare(String[] args) throws BadNodeUrlException, TException {
    ConfigNodeInfo.getInstance()
        .updateConfigNodeList(IoTDBDescriptor.getInstance().getConfig().getTargetConfigNodeList());
    try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
      TDataNodeConfigurationResp resp = configNodeClient.getDataNodeConfiguration(-1);
      // 1. online Data Node size - removed Data Node size < replication,NOT ALLOW remove
      //   But replication size is set in Config Node's configuration, so check it in remote Config
      // Node

      // 2. removed Data Node IP not contained in below map, CAN NOT remove.
      Map<Integer, TDataNodeConfiguration> nodeIdToNodeConfiguration =
          resp.getDataNodeConfigurationMap();
      List<TEndPoint> endPoints = NodeUrlUtils.parseTEndPointUrls(args[1]);
      List<String> removedDataNodeIps =
          endPoints.stream().map(TEndPoint::getIp).collect(Collectors.toList());

      List<String> onlineDataNodeIps =
          nodeIdToNodeConfiguration.values().stream()
              .map(TDataNodeConfiguration::getLocation)
              .map(TDataNodeLocation::getInternalEndPoint)
              .map(TEndPoint::getIp)
              .collect(Collectors.toList());
      IoTDBStopCheck.getInstance().checkIpInCluster(removedDataNodeIps, onlineDataNodeIps);
    }
  }

  private void removeNodesFromCluster(String[] args)
      throws BadNodeUrlException, TException, IoTDBException {
    logger.info("start to remove DataNode from cluster");
    List<TDataNodeLocation> dataNodeLocations = buildDataNodeLocations(args[1]);
    if (dataNodeLocations.isEmpty()) {
      throw new BadNodeUrlException("build DataNode location is empty");
    }
    logger.info(
        "there has data nodes location will be removed. size is: {}, detail: {}",
        dataNodeLocations.size(),
        dataNodeLocations);
    TDataNodeRemoveReq removeReq = new TDataNodeRemoveReq(dataNodeLocations);
    try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
      TDataNodeRemoveResp removeResp = configNodeClient.removeDataNode(removeReq);
      logger.info("Remove result {} ", removeResp.toString());
      if (removeResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBException(
            removeResp.getStatus().toString(), removeResp.getStatus().getCode());
      }
      logger.info(
          "Submit remove datanode request successfully, "
              + "more details are shown in the logs of confignode-leader and removed-datanode, "
              + "and after the process of remove-datanode is over, "
              + "you are supposed to delete directory and data of the removed-datanode manually");
    }
  }

  /**
   * fetch all datanode info from ConfigNode, then compare with input 'ips'
   *
   * @param endPorts data node ip:port, split with ','
   * @return TDataNodeLocation list
   */
  private List<TDataNodeLocation> buildDataNodeLocations(String endPorts)
      throws BadNodeUrlException {
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    if (endPorts == null || endPorts.trim().isEmpty()) {
      return dataNodeLocations;
    }

    List<TEndPoint> endPoints = NodeUrlUtils.parseTEndPointUrls(endPorts);

    try (ConfigNodeClient client = new ConfigNodeClient()) {
      dataNodeLocations =
          client.getDataNodeConfiguration(-1).getDataNodeConfigurationMap().values().stream()
              .map(TDataNodeConfiguration::getLocation)
              .filter(location -> endPoints.contains(location.getClientRpcEndPoint()))
              .collect(Collectors.toList());
    } catch (TException e) {
      logger.error("get data node locations failed", e);
    }

    if (endPoints.size() != dataNodeLocations.size()) {
      logger.error(
          "build DataNode locations error, "
              + "because number of input DataNode({}) NOT EQUALS the number of fetched DataNodeLocations({}), will return empty locations",
          endPoints.size(),
          dataNodeLocations.size());
      dataNodeLocations.clear();
      return dataNodeLocations;
    }

    return dataNodeLocations;
  }

  private void removeTail() {}
}
