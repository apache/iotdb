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

package org.apache.iotdb.cluster.server.clusterinfo;

import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.ClusterInfoService.Processor;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.RPCServiceException;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.service.thrift.ThriftService;
import org.apache.iotdb.db.service.thrift.ThriftServiceThread;

public class ClusterInfoServer extends ThriftService implements ClusterInfoServerMBean {
  private ClusterInfoServiceImpl serviceImpl;

  public static ClusterInfoServer getInstance() {
    return ClusterMonitorServiceHolder.INSTANCE;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CLUSTER_INFO_SERVICE;
  }

  @Override
  public ThriftService getImplementation() {
    return getInstance();
  }

  @Override
  public void initTProcessor() {
    serviceImpl = new ClusterInfoServiceImpl();
    processor = new Processor<>(serviceImpl);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    ClusterConfig clusterConfig = ClusterDescriptor.getInstance().getConfig();
    IoTDBConfig nodeConfig = IoTDBDescriptor.getInstance().getConfig();
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.CLUSTER_INFO_SERVICE.getName(),
              nodeConfig.getRpcAddress(),
              clusterConfig.getClusterInfoRpcPort(),
              nodeConfig.getRpcMaxConcurrentClientNum(),
              nodeConfig.getThriftServerAwaitTimeForStopService(),
              new ClusterInfoServiceThriftHandler(serviceImpl),
              IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable());
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.CLUSTER_INFO_SERVICE.getName() + "Service");
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return ClusterDescriptor.getInstance().getConfig().getClusterInfoRpcPort();
  }

  private static class ClusterMonitorServiceHolder {

    private static final ClusterInfoServer INSTANCE = new ClusterInfoServer();

    private ClusterMonitorServiceHolder() {}
  }
}
