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
package org.apache.iotdb.confignode.service.thrift.server;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;

/** ConfigNodeRPCServer exposes the interface that interacts with the DataNode */
public class ConfigNodeRPCServer extends ThriftService implements ConfigNodeRPCServerMBean {
  private final ConfigNodeConf config = ConfigNodeDescriptor.getInstance().getConf();

  private ConfigNodeRPCServerProcessor configNodeRPCServerProcessor;

  private ConfigNodeRPCServer() {}

  @Override
  public ThriftService getImplementation() {
    return ConfigNodeRPCServer.getInstance();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CONFIG_NODE_SERVICE;
  }

  @Override
  public void initSyncedServiceImpl(Object configNodeRPCServerProcessor) {
    this.configNodeRPCServerProcessor = (ConfigNodeRPCServerProcessor) configNodeRPCServerProcessor;

    super.mbeanName =
        String.format(
            "%s:%s=%s", this.getClass().getPackage(), IoTDBConstant.JMX_TYPE, getID().getJmxName());
    super.initSyncedServiceImpl(this.configNodeRPCServerProcessor);
  }

  @Override
  public void initTProcessor() throws InstantiationException {
    processor = new ConfigIService.Processor<>(configNodeRPCServerProcessor);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {

    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.CONFIG_NODE_RPC_CLIENT.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new ConfigNodeRPCServiceHandler(configNodeRPCServerProcessor),
              config.isRpcThriftCompressionEnabled());
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.CONFIG_NODE_RPC_SERVER.getName());
  }

  @Override
  public String getBindIP() {
    return config.getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return config.getRpcPort();
  }

  public static ConfigNodeRPCServer getInstance() {
    return ConfigNodeRPCServerHolder.INSTANCE;
  }

  private static class ConfigNodeRPCServerHolder {

    private static final ConfigNodeRPCServer INSTANCE = new ConfigNodeRPCServer();

    private ConfigNodeRPCServerHolder() {}
  }
}
