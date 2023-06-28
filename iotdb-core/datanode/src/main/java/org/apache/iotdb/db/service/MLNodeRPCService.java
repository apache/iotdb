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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.thrift.handler.MLNodeRPCServiceThriftHandler;
import org.apache.iotdb.db.protocol.thrift.impl.MLNodeRPCServiceImpl;
import org.apache.iotdb.mpp.rpc.thrift.IMLNodeInternalRPCService;

import java.lang.reflect.InvocationTargetException;

public class MLNodeRPCService extends ThriftService implements MLNodeRPCServiceMBean {

  private MLNodeRPCServiceImpl impl;

  private MLNodeRPCService() {}

  @Override
  public ServiceType getID() {
    return ServiceType.MLNode_RPC_SERVICE;
  }

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException,
          NoSuchMethodException, InvocationTargetException {
    impl = new MLNodeRPCServiceImpl();
    initSyncedServiceImpl(null);
    processor = new IMLNodeInternalRPCService.Processor<>(impl);
  }

  @Override
  public void initThriftServiceThread()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    try {
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.MLNODE_RPC_SERVICE.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new MLNodeRPCServiceThriftHandler(impl),
              // TODO: hard coded compress strategy
              false);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.MLNODE_RPC_SERVICE.getName());
    // TODO: metricService
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return IoTDBDescriptor.getInstance().getConfig().getMLNodePort();
  }

  private static class MLNodeRPCServiceHolder {
    private static final MLNodeRPCService INSTANCE = new MLNodeRPCService();

    private MLNodeRPCServiceHolder() {}
  }

  public static MLNodeRPCService getInstance() {
    return MLNodeRPCService.MLNodeRPCServiceHolder.INSTANCE;
  }
}
