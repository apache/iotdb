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

package org.apache.iotdb.consensus.multileader.service;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.consensus.multileader.conf.MultiLeaderConsensusConfig;
import org.apache.iotdb.consensus.multileader.thrift.MultiLeaderConsensusIService;

import java.lang.reflect.InvocationTargetException;

public class MultiLeaderRPCService extends ThriftService implements MultiLeaderRPCServiceMBean {

  private final TEndPoint thisNode;
  private MultiLeaderRPCServiceProcessor multiLeaderRPCServiceProcessor;

  public MultiLeaderRPCService(TEndPoint thisNode) {
    this.thisNode = thisNode;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MULTI_LEADER_CONSENSUS_SERVICE;
  }

  @Override
  public void initSyncedServiceImpl(Object multiLeaderRPCServiceProcessor) {
    this.multiLeaderRPCServiceProcessor =
        (MultiLeaderRPCServiceProcessor) multiLeaderRPCServiceProcessor;
    super.mbeanName =
        String.format(
            "%s:%s=%s", this.getClass().getPackage(), IoTDBConstant.JMX_TYPE, getID().getJmxName());
    super.initSyncedServiceImpl(this.multiLeaderRPCServiceProcessor);
  }

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException,
          NoSuchMethodException, InvocationTargetException {
    processor = new MultiLeaderConsensusIService.Processor<>(multiLeaderRPCServiceProcessor);
  }

  @Override
  public void initThriftServiceThread()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.MULTI_LEADER_CONSENSUS_RPC_CLIENT.getName(),
              getBindIP(),
              getBindPort(),
              MultiLeaderConsensusConfig.RPC_MAX_CONCURRENT_CLIENT_NUM,
              MultiLeaderConsensusConfig.THRIFT_SERVER_AWAIT_TIME_FOR_STOP_SERVICE,
              new MultiLeaderRPCServiceHandler(multiLeaderRPCServiceProcessor),
              MultiLeaderConsensusConfig.IS_RPC_THRIFT_COMPRESSION_ENABLED);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.MULTI_LEADER_CONSENSUS_RPC_SERVER.getName());
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
