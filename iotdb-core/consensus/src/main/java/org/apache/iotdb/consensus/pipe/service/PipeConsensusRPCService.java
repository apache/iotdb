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
import org.apache.iotdb.consensus.config.PipeConsensusConfig;
import org.apache.iotdb.consensus.pipe.thrift.PipeConsensusIService;
import org.apache.iotdb.rpc.ZeroCopyRpcTransportFactory;

public class PipeConsensusRPCService extends ThriftService implements PipeConsensusRPCServiceMBean {

  private final TEndPoint thisNode;
  private final PipeConsensusConfig config;
  private PipeConsensusRPCServiceProcessor pipeConsensusRPCServiceProcessor;

  public PipeConsensusRPCService(TEndPoint thisNode, PipeConsensusConfig config) {
    this.thisNode = thisNode;
    this.config = config;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PIPE_CONSENSUS_SERVICE;
  }

  @Override
  public void initSyncedServiceImpl(Object pipeConsensusRPCServiceProcessor) {
    this.pipeConsensusRPCServiceProcessor =
        (PipeConsensusRPCServiceProcessor) pipeConsensusRPCServiceProcessor;
    super.initSyncedServiceImpl(this.pipeConsensusRPCServiceProcessor);
  }

  @Override
  public void initTProcessor() {
    processor = new PipeConsensusIService.Processor<>(pipeConsensusRPCServiceProcessor);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.PIPE_CONSENSUS_RPC_PROCESSOR.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpc().getRpcMaxConcurrentClientNum(),
              config.getRpc().getThriftServerAwaitTimeForStopService(),
              new PipeConsensusRPCServiceHandler(pipeConsensusRPCServiceProcessor),
              config.getRpc().isRpcThriftCompressionEnabled(),
              ZeroCopyRpcTransportFactory.INSTANCE);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.PIPE_CONSENSUS_RPC_SERVICE.getName());
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
