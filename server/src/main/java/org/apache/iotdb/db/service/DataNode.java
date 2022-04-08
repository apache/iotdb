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

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.utils.CommonUtils;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterResp;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConfigCheck;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.thrift.impl.DataNodeManagementServiceImpl;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class DataNode implements DataNodeMBean {
  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

  private final String mbeanName =
      String.format(
          "%s:%s=%s", "org.apache.iotdb.datanode.service", IoTDBConstant.JMX_TYPE, "DataNode");

  /**
   * when joining a cluster this node will retry at most "DEFAULT_JOIN_RETRY" times before returning
   * a failure to the client
   */
  private static final int DEFAULT_JOIN_RETRY = 10;

  private Endpoint thisNode = new Endpoint();

  private int dataNodeID;

  private DataNode() {
    // we do not init anything here, so that we can re-initialize the instance in IT.
  }

  private final IoTDB iotdb = IoTDB.getInstance();

  private final RegisterManager registerManager = new RegisterManager();

  // private IClientManager clientManager;

  public static DataNode getInstance() {
    return DataNodeHolder.INSTANCE;
  }

  public static void main(String[] args) {
    new DataNodeServerCommandLine().doMain(args);
  }

  protected void serverCheckAndInit() throws ConfigurationException, IOException {
    IoTDBConfigCheck.getInstance().checkConfig();
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

    config.setSyncEnable(false);
    // TODO: check configuration for data node

    // if client ip is the default address, set it same with internal ip
    if (config.getRpcAddress().equals("0.0.0.0")) {
      config.setRpcAddress(config.getInternalIp());
    }

    thisNode.setIp(IoTDBDescriptor.getInstance().getConfig().getInternalIp());
    thisNode.setPort(IoTDBDescriptor.getInstance().getConfig().getInternalPort());
  }

  protected void doAddNode(String[] args) {
    try {
      // TODO : contact with config node to join into the cluster
      joinCluster();
      active();
    } catch (StartupException e) {
      logger.error("Fail to start  server", e);
      stop();
    }
  }

  protected void doRemoveNode(String[] args) {
    // TODO: remove data node
  }

  /** initialize the current node and its services */
  public boolean initLocalEngines() {
    IoTDB.setClusterMode();
    return true;
  }

  public void joinCluster() throws StartupException {
    List<Endpoint> configNodes;
    try {
      configNodes =
          CommonUtils.parseNodeUrls(IoTDBDescriptor.getInstance().getConfig().getConfigNodeUrls());
    } catch (BadNodeUrlException e) {
      throw new StartupException(e.getMessage());
    }

    int retry = DEFAULT_JOIN_RETRY;
    while (retry > 0) {
      // randomly pick up a config node to try
      Random random = new Random();
      Endpoint configNode = configNodes.get(random.nextInt(configNodes.size()));
      logger.info("start joining the cluster with the help of {}", configNode);
      try {
        ConfigIService.Client client = createClient(configNode);
        DataNodeRegisterResp dataNodeRegisterResp =
            client.registerDataNode(
                new DataNodeRegisterReq(new EndPoint(thisNode.getIp(), thisNode.getPort())));
        if (dataNodeRegisterResp.getRegisterResult().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          dataNodeID = dataNodeRegisterResp.getDataNodeID();
          logger.info("Joined a cluster successfully");
          return;
        }
        // wait 5s to start the next try
        Thread.sleep(IoTDBDescriptor.getInstance().getConfig().getJoinClusterTimeOutMs());
      } catch (TException | IoTDBConnectionException e) {
        logger.warn("Cannot join the cluster from {}, because:", configNode, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to join a cluster", e);
      }
      // start the next try
      retry--;
    }
    // all tries failed
    logger.error("Cannot join the cluster after {} retries", DEFAULT_JOIN_RETRY);
    throw new StartupException("Can not connect with the config nodes, please check the network");
  }

  public void active() throws StartupException {
    // start iotdb server first
    IoTDB.getInstance().active();

    /** Register services */
    JMXService.registerMBean(getInstance(), mbeanName);
    // TODO: move rpc service initialization from iotdb instance here
    DataNodeManagementServiceImpl dataNodeInternalServiceImpl = new DataNodeManagementServiceImpl();
    DataNodeManagementServer.getInstance().initSyncedServiceImpl(dataNodeInternalServiceImpl);
    registerManager.register(DataNodeManagementServer.getInstance());
    // init influxDB MManager
    if (IoTDBDescriptor.getInstance().getConfig().isEnableInfluxDBRpcService()) {
      IoTDB.initInfluxDBMManager();
    }
  }

  public void stop() {
    deactivate();
  }

  private void deactivate() {
    logger.info("Deactivating data node...");
    // stopThreadPools();
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    logger.info("Data node is deactivated.");
    // stop the iotdb kernel
    iotdb.stop();
  }

  private static class DataNodeHolder {

    private static final DataNode INSTANCE = new DataNode();

    private DataNodeHolder() {}
  }

  private ConfigIService.Client createClient(Endpoint endpoint) throws IoTDBConnectionException {
    TTransport transport;
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // as there is a try-catch already, we do not need to use TSocket.wrap
              endpoint.getIp(), endpoint.getPort(), 2000);
      transport.open();
    } catch (TTransportException e) {
      throw new IoTDBConnectionException(e);
    }

    ConfigIService.Client client;
    if (IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()) {
      client = new ConfigIService.Client(new TCompactProtocol(transport));
    } else {
      client = new ConfigIService.Client(new TBinaryProtocol(transport));
    }
    return client;
  }
}
