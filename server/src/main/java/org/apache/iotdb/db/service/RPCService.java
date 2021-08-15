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

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.RPCServiceException;
import org.apache.iotdb.db.service.thrift.ThriftService;
import org.apache.iotdb.db.service.thrift.ThriftServiceThread;
import org.apache.iotdb.service.rpc.thrift.TSIService.Processor;

/** A service to handle jdbc request from client. */
public class RPCService extends ThriftService implements RPCServiceMBean {

  private TSServiceImpl impl;

  private RPCService() {}

  public static RPCService getInstance() {
    return RPCServiceHolder.INSTANCE;
  }

  @Override
  public int getRPCPort() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    return config.getRpcPort();
  }

  @Override
  public ThriftService getImplementation() {
    return getInstance();
  }

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    impl =
        (TSServiceImpl)
            Class.forName(IoTDBDescriptor.getInstance().getConfig().getRpcImplClassName())
                .newInstance();
    processor = new Processor<>(impl);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.RPC_CLIENT.getName(),
              config.getRpcAddress(),
              config.getRpcPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new RPCServiceThriftHandler(impl),
              IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable());
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.RPC_SERVICE.getName());
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

  private static class RPCServiceHolder {

    private static final RPCService INSTANCE = new RPCService();

    private RPCServiceHolder() {}
  }
}
