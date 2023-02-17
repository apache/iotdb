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
package org.apache.iotdb.confignode.service.thrift;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;

/** ConfigNodeRPCServer exposes the interface that interacts with the DataNode */
public class ConfigNodeRPCService extends ThriftService implements ConfigNodeRPCServiceMBean {

  private static final ConfigNodeConfig configConf = ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private ConfigNodeRPCServiceProcessor configNodeRPCServiceProcessor;

  @Override
  public ServiceType getID() {
    return ServiceType.CONFIG_NODE_SERVICE;
  }

  @Override
  public void initSyncedServiceImpl(Object configNodeRPCServerProcessor) {
    this.configNodeRPCServiceProcessor =
        (ConfigNodeRPCServiceProcessor) configNodeRPCServerProcessor;

    super.mbeanName =
        String.format(
            "%s:%s=%s", this.getClass().getPackage(), IoTDBConstant.JMX_TYPE, getID().getJmxName());
    super.initSyncedServiceImpl(this.configNodeRPCServiceProcessor);
  }

  @Override
  public void initTProcessor() throws InstantiationException {
    processor = new IConfigNodeRPCService.Processor<>(configNodeRPCServiceProcessor);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {

    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.CONFIGNODE_RPC_PROCESSOR.getName(),
              getBindIP(),
              getBindPort(),
              configConf.getCnRpcMaxConcurrentClientNum(),
              configConf.getThriftServerAwaitTimeForStopService(),
              new ConfigNodeRPCServiceHandler(),
              commonConfig.isRpcThriftCompressionEnabled());
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.CONFIGNODE_RPC_SERVICE.getName());
    MetricService.getInstance().addMetricSet(new ConfigNodeRPCServiceMetrics(thriftServiceThread));
  }

  @Override
  public String getBindIP() {
    return configConf.getInternalAddress();
  }

  @Override
  public int getBindPort() {
    return configConf.getInternalPort();
  }
}
