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
import org.apache.iotdb.db.protocol.thrift.handler.AINodeRPCServiceThriftHandler;
import org.apache.iotdb.db.protocol.thrift.impl.AINodeRPCServiceImpl;
import org.apache.iotdb.mpp.rpc.thrift.IAINodeInternalRPCService;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;

public class AINodeRPCService extends ThriftService implements AINodeRPCServiceMBean {

  private AINodeRPCServiceImpl impl;

  private AINodeRPCService() {}

  @Override
  public ServiceType getID() {
    return ServiceType.AINode_RPC_SERVICE;
  }

  @Override
  public void initTProcessor() {
    impl = new AINodeRPCServiceImpl();
    initSyncedServiceImpl(null);
    processor = new IAINodeInternalRPCService.Processor<>(impl);
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
              ThreadName.AINODE_RPC_SERVICE.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new AINodeRPCServiceThriftHandler(impl),
              config.isRpcThriftCompressionEnable(),
              DeepCopyRpcTransportFactory.INSTANCE);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.AINODE_RPC_SERVICE.getName());
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return IoTDBDescriptor.getInstance().getConfig().getAINodePort();
  }

  private static class AINodeRPCServiceHolder {
    private static final AINodeRPCService INSTANCE = new AINodeRPCService();

    private AINodeRPCServiceHolder() {}
  }

  public static AINodeRPCService getInstance() {
    return AINodeRPCServiceHolder.INSTANCE;
  }
}
