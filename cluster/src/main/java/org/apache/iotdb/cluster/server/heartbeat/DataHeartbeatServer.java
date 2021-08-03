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

package org.apache.iotdb.cluster.server.heartbeat;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService.AsyncProcessor;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService.Processor;
import org.apache.iotdb.cluster.server.DataClusterServer;
import org.apache.iotdb.cluster.utils.ClusterUtils;

import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.InetSocketAddress;

public class DataHeartbeatServer extends HeartbeatServer {

  private DataClusterServer dataClusterServer;

  /** Do not use this method for initialization */
  private DataHeartbeatServer() {}

  public DataHeartbeatServer(Node thisNode, DataClusterServer dataClusterServer) {
    super(thisNode);
    this.dataClusterServer = dataClusterServer;
  }

  @Override
  TProcessor getProcessor() {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      return new AsyncProcessor<>(dataClusterServer);
    } else {
      return new Processor<>(dataClusterServer);
    }
  }

  @Override
  TServerTransport getHeartbeatServerSocket() throws TTransportException {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      return new TNonblockingServerSocket(
          new InetSocketAddress(
              config.getInternalIp(),
              thisNode.getDataPort() + ClusterUtils.DATA_HEARTBEAT_PORT_OFFSET),
          getConnectionTimeoutInMS());
    } else {
      return new TServerSocket(
          new InetSocketAddress(
              config.getInternalIp(),
              thisNode.getDataPort() + ClusterUtils.DATA_HEARTBEAT_PORT_OFFSET));
    }
  }

  @Override
  String getClientThreadPrefix() {
    return "DataHeartbeatClientThread-";
  }

  @Override
  String getServerClientName() {
    return "DataHeartbeatServerThread-";
  }
}
