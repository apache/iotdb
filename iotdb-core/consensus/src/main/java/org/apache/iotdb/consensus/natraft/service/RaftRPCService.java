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

package org.apache.iotdb.consensus.natraft.service;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.raft.thrift.RaftService.AsyncProcessor;

import org.apache.thrift.TBaseAsyncProcessor;

public class RaftRPCService extends ThriftService implements RaftRPCServiceMBean {

  private final TEndPoint thisNode;
  private final RaftConfig config;
  private RaftRPCServiceProcessor raftRPCServiceProcessor;

  public RaftRPCService(TEndPoint thisNode, RaftConfig config) {
    this.thisNode = thisNode;
    this.config = config;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.RAFT_CONSENSUS_SERVICE;
  }

  @Override
  public void initAsyncedServiceImpl(Object raftRPCServiceProcessor) {
    this.raftRPCServiceProcessor = (RaftRPCServiceProcessor) raftRPCServiceProcessor;
    super.mbeanName =
        String.format(
            "%s:%s=%s", this.getClass().getPackage(), IoTDBConstant.JMX_TYPE, getID().getJmxName());
    super.initAsyncedServiceImpl(this.raftRPCServiceProcessor);
  }

  @Override
  public void initTProcessor() {
    processor = new AsyncProcessor<>(raftRPCServiceProcessor);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              (TBaseAsyncProcessor) processor,
              getID().getName(),
              ThreadName.RAFT_CONSENSUS_RPC_PROCESSOR.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpcConfig().getRpcSelectorThreadNum(),
              config.getRpcConfig().getRpcMinConcurrentClientNum(),
              config.getRpcConfig().getRpcMaxConcurrentClientNum(),
              config.getRpcConfig().getThriftServerAwaitTimeForStopService(),
              new RaftRPCServiceHandler(raftRPCServiceProcessor),
              config.getRpcConfig().isRpcThriftCompressionEnabled(),
              config.getRpcConfig().getConnectionTimeoutInMs(),
              config.getRpcConfig().getThriftMaxFrameSize(),
              ThriftServiceThread.ServerType.SELECTOR);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.RAFT_CONSENSUS_RPC_SERVICE.getName());
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
