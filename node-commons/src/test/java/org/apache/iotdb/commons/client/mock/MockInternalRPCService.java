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

package org.apache.iotdb.commons.client.mock;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;

import org.apache.thrift.server.TServerEventHandler;

import static org.mockito.Mockito.mock;

public class MockInternalRPCService extends ThriftService implements MockInternalRPCServiceMBean {

  private final TEndPoint thisNode;
  private IDataNodeRPCService.Iface mockedProcessor;

  public MockInternalRPCService(TEndPoint thisNode) {
    this.thisNode = thisNode;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.INTERNAL_SERVICE;
  }

  @Override
  public void initSyncedServiceImpl(Object mockedProcessor) {
    this.mockedProcessor = (IDataNodeRPCService.Iface) mockedProcessor;
    super.mbeanName =
        String.format(
            "%s:%s=%s", this.getClass().getPackage(), IoTDBConstant.JMX_TYPE, getID().getJmxName());
    super.initSyncedServiceImpl(this.mockedProcessor);
  }

  @Override
  public void initTProcessor() {
    processor = new IDataNodeRPCService.Processor<>(mockedProcessor);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.DATANODE_INTERNAL_RPC_SERVICE.getName(),
              getBindIP(),
              getBindPort(),
              65535,
              60,
              mock(TServerEventHandler.class),
              false);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.DATANODE_INTERNAL_RPC_SERVICE.getName());
  }

  @Override
  public String getBindIP() {
    return thisNode.getIp();
  }

  @Override
  public int getBindPort() {
    return thisNode.getPort();
  }
}
