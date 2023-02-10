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

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.thrift.ProcessorWithMetrics;
import org.apache.iotdb.db.service.thrift.handler.RPCServiceThriftHandler;
import org.apache.iotdb.db.service.thrift.impl.IClientRPCServiceWithHandler;

import java.lang.reflect.InvocationTargetException;

/** A service to handle RPC request from client. */
public class RPCService extends ThriftService implements RPCServiceMBean {

  private IClientRPCServiceWithHandler impl;

  public static RPCService getInstance() {
    return RPCServiceHolder.INSTANCE;
  }

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException,
          NoSuchMethodException, InvocationTargetException {
    impl =
        (IClientRPCServiceWithHandler)
            Class.forName(IoTDBDescriptor.getInstance().getConfig().getRpcImplClassName())
                .getDeclaredConstructor()
                .newInstance();
    initSyncedServiceImpl(null);
    processor = new ProcessorWithMetrics(impl);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.CLIENT_RPC_PROCESSOR.getName(),
              config.getRpcAddress(),
              config.getRpcPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new RPCServiceThriftHandler(impl),
              IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable());
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.CLIENT_RPC_SERVICE.getName());
    MetricService.getInstance().addMetricSet(new RPCServiceMetrics(thriftServiceThread));
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return IoTDBDescriptor.getInstance().getConfig().getRpcPort();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.RPC_SERVICE;
  }

  @Override
  public int getRPCPort() {
    return getBindPort();
  }

  private static class RPCServiceHolder {

    private static final RPCService INSTANCE = new RPCService();

    private RPCServiceHolder() {}
  }
}
