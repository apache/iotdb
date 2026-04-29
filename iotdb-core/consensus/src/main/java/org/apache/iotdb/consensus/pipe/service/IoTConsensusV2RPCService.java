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

package org.apache.iotdb.consensus.pipe.service;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.consensus.config.IoTConsensusV2Config;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.IoTConsensusV2IService;
import org.apache.iotdb.rpc.ZeroCopyRpcTransportFactory;

public class IoTConsensusV2RPCService extends ThriftService
    implements IoTConsensusV2RPCServiceMBean {

  private final TEndPoint thisNode;
  private final IoTConsensusV2Config config;
  private IoTConsensusV2RPCServiceProcessor iotConsensusV2RPCServiceProcessor;

  public IoTConsensusV2RPCService(TEndPoint thisNode, IoTConsensusV2Config config) {
    this.thisNode = thisNode;
    this.config = config;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.IOT_CONSENSUS_V2_SERVICE;
  }

  @Override
  public void initSyncedServiceImpl(Object iotConsensusV2RPCServiceProcessor) {
    this.iotConsensusV2RPCServiceProcessor =
        (IoTConsensusV2RPCServiceProcessor) iotConsensusV2RPCServiceProcessor;
    super.initSyncedServiceImpl(this.iotConsensusV2RPCServiceProcessor);
  }

  @Override
  public void initTProcessor() {
    processor = new IoTConsensusV2IService.Processor<>(iotConsensusV2RPCServiceProcessor);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    try {
      thriftServiceThread =
          config.getRpc().isEnableSSL()
              ? new ThriftServiceThread(
                  processor,
                  getID().getName(),
                  ThreadName.IOT_CONSENSUS_V2_RPC_PROCESSOR.getName(),
                  getBindIP(),
                  getBindPort(),
                  config.getRpc().getRpcMaxConcurrentClientNum(),
                  config.getRpc().getThriftServerAwaitTimeForStopService(),
                  new IoTConsensusV2RPCServiceHandler(iotConsensusV2RPCServiceProcessor),
                  config.getRpc().isRpcThriftCompressionEnabled(),
                  config.getRpc().getSslKeyStorePath(),
                  config.getRpc().getSslKeyStorePassword(),
                  config.getRpc().getSslTrustStorePath(),
                  config.getRpc().getSslTrustStorePassword(),
                  ZeroCopyRpcTransportFactory.INSTANCE)
              : new ThriftServiceThread(
                  processor,
                  getID().getName(),
                  ThreadName.IOT_CONSENSUS_V2_RPC_PROCESSOR.getName(),
                  getBindIP(),
                  getBindPort(),
                  config.getRpc().getRpcMaxConcurrentClientNum(),
                  config.getRpc().getThriftServerAwaitTimeForStopService(),
                  new IoTConsensusV2RPCServiceHandler(iotConsensusV2RPCServiceProcessor),
                  config.getRpc().isRpcThriftCompressionEnabled(),
                  ZeroCopyRpcTransportFactory.INSTANCE);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.IOT_CONSENSUS_V2_RPC_SERVICE.getName());
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
