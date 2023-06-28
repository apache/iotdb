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

package org.apache.iotdb.consensus.iot.service;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.thrift.IoTConsensusIService;

import org.apache.thrift.TBaseAsyncProcessor;

import java.lang.reflect.InvocationTargetException;

public class IoTConsensusRPCService extends ThriftService implements IoTConsensusRPCServiceMBean {

  private final TEndPoint thisNode;
  private final IoTConsensusConfig config;
  private IoTConsensusRPCServiceProcessor iotConsensusRPCServiceProcessor;

  public IoTConsensusRPCService(TEndPoint thisNode, IoTConsensusConfig config) {
    this.thisNode = thisNode;
    this.config = config;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.IOT_CONSENSUS_SERVICE;
  }

  @Override
  public void initAsyncedServiceImpl(Object iotConsensusRPCServiceProcessor) {
    this.iotConsensusRPCServiceProcessor =
        (IoTConsensusRPCServiceProcessor) iotConsensusRPCServiceProcessor;
    super.mbeanName =
        String.format(
            "%s:%s=%s", this.getClass().getPackage(), IoTDBConstant.JMX_TYPE, getID().getJmxName());
    super.initAsyncedServiceImpl(this.iotConsensusRPCServiceProcessor);
  }

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException,
          NoSuchMethodException, InvocationTargetException {
    processor = new IoTConsensusIService.AsyncProcessor<>(iotConsensusRPCServiceProcessor);
  }

  @Override
  public void initThriftServiceThread()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              (TBaseAsyncProcessor<?>) processor,
              getID().getName(),
              ThreadName.IOT_CONSENSUS_RPC_PROCESSOR.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpc().getRpcSelectorThreadNum(),
              config.getRpc().getRpcMinConcurrentClientNum(),
              config.getRpc().getRpcMaxConcurrentClientNum(),
              config.getRpc().getThriftServerAwaitTimeForStopService(),
              new IoTConsensusRPCServiceHandler(iotConsensusRPCServiceProcessor),
              config.getRpc().isRpcThriftCompressionEnabled(),
              config.getRpc().getConnectionTimeoutInMs(),
              config.getRpc().getThriftMaxFrameSize(),
              ThriftServiceThread.ServerType.SELECTOR);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.IOT_CONSENSUS_RPC_SERVICE.getName());
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
