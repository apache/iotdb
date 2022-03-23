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
package org.apache.iotdb.db.datanode;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConfigCheck;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IoTDB;

import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataNode implements DataNodeMBean {
  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

  private final String mbeanName =
      String.format(
          "%s:%s=%s", "org.apache.iotdb.datanode.service", IoTDBConstant.JMX_TYPE, "DataNode");

  private DataNode() {
    // we do not init anything here, so that we can re-initialize the instance in IT.
  }

  private final IoTDB iotdb = IoTDB.getInstance();

  private final RegisterManager registerManager = new RegisterManager();

  // private IClientManager clientManager;

  public static DataNode getInstance() {
    return DataNodeHolder.INSTANCE;
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
  }

  protected void doAddNode(String[] args) {}

  protected void doRemoveNode(String[] args) {
    // TODO: remove data node
  }

  /** initialize the current node and its services */
  public boolean initLocalEngines() {
    //  ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    //    thisNode = new Node();
    //    // set internal rpc ip and ports
    //    thisNode.setInternalIp(config.getInternalIp());
    //    thisNode.setMetaPort(config.getInternalMetaPort());
    //    thisNode.setDataPort(config.getInternalDataPort());
    //    // set client rpc ip and ports
    //    thisNode.setClientPort(config.getClusterRpcPort());
    //    thisNode.setClientIp(IoTDBDescriptor.getInstance().getConfig().getRpcAddress());

    // local engine
    TProtocolFactory protocolFactory =
        ThriftServiceThread.getProtocolFactory(
            IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable());
    IoTDB.setClusterMode();
    //    IoTDB.setSchemaEngine(CSchemaEngine.getInstance());
    //    ((CSchemaEngine) IoTDB.schemaEngine).setMetaGroupMember(metaGroupMember);
    //    ((CSchemaEngine) IoTDB.schemaEngine).setCoordinator(coordinator);
    // MetaPuller.getInstance().init(metaGroupMember);

    // try {
    // IoTDB.setServiceProvider(new ServiceProvider(coordinator, metaGroupMember));
    //    } catch (QueryProcessException e) {
    //      logger.error("Failed to set clusterServiceProvider.", e);
    //      stop();
    //      return false;
    //    }

    // from the scope of the DataGroupEngine,it should be singleton pattern
    // the way of setting MetaGroupMember in DataGroupEngine may need a better modification in
    // future commit.
    //    DataGroupEngine.setProtocolFactory(protocolFactory);
    //    DataGroupEngine.setMetaGroupMember(metaGroupMember);
    //    dataGroupEngine = DataGroupEngine.getInstance();
    //    clientManager =
    //            new ClientManager(
    //                    ClusterDescriptor.getInstance().getConfig().isUseAsyncServer(),
    //                    ClientManager.Type.RequestForwardClient);
    // initTasks();
    try {
      // we need to check config after initLocalEngines.
      startServerCheck();
      registerManager.register(JMXService.getInstance());
      JMXService.registerMBean(metaGroupMember, metaGroupMember.getMBeanName());
    } catch (StartupException e) {
      logger.error("Failed to check cluster config.", e);
      stop();
      return false;
    }
    return true;
  }

  public void active() {
    // start iotdb server first
    IoTDB.getInstance().active();

    // rpc service initialize

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
}
