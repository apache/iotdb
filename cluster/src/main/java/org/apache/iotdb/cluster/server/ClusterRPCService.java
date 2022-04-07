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

package org.apache.iotdb.cluster.server;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.thrift.ProcessorWithMetrics;
import org.apache.iotdb.db.service.thrift.handler.RPCServiceThriftHandler;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.service.rpc.thrift.TSIService.Processor;

public class ClusterRPCService extends ThriftService implements ClusterRPCServiceMBean {

  private ClusterTSServiceImpl impl;

  private ClusterRPCService() {}

  @Override
  public ThriftService getImplementation() {
    return ClusterRPCServiceHolder.INSTANCE;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CLUSTER_RPC_SERVICE;
  }

  @Override
  public void initSyncedServiceImpl(Object serviceImpl) {
    impl = (ClusterTSServiceImpl) serviceImpl;
    super.initSyncedServiceImpl(serviceImpl);
  }

  @Override
  public void initTProcessor() throws InstantiationException {
    if (impl == null) {
      throw new InstantiationException("ClusterTSServiceImpl is null");
    }
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      processor = new ProcessorWithMetrics(impl);
    } else {
      processor = new Processor<>(impl);
    }
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.CLUSTER_RPC_CLIENT.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new RPCServiceThriftHandler(impl),
              IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable());
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.CLUSTER_RPC_SERVICE.getName());
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return ClusterDescriptor.getInstance().getConfig().getClusterRpcPort();
  }

  @Override
  public int getRPCPort() {
    return getBindPort();
  }

  public static ClusterRPCService getInstance() {
    return ClusterRPCServiceHolder.INSTANCE;
  }

  private static class ClusterRPCServiceHolder {

    private static final ClusterRPCService INSTANCE = new ClusterRPCService();

    private ClusterRPCServiceHolder() {}
  }
}
