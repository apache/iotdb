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
package org.apache.iotdb.confignode.consensus;

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeMessageResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupMessage;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupMessageResp;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Demo for ConfigNode's integration with the ratis-consensus protocol. */
public class RatisConsensusDemo {

  private static final String localhost = "0.0.0.0";
  private static final int timeOutInMS = 2000;

  private ConfigIService.Client[] clients;

  /**
   * To run this code, follow these steps: 1. Compile IoTDB 2. Copy at least three
   * iotdb-confignode-0.14.0-SNAPSHOT 3. Make sure these parameters: config_node_rpc_address(all
   * 0.0.0.0), config_node_rpc_port(22277, 22279, 22281), config_node_internal_port(22278, 22280,
   * 22282), consensus_type(all ratis) and config_node_group_address_list(all 0.0.0.0:22278,
   * 0.0.0.0:22280, 0.0.0.0:22282) in each iotdb-confignode.properties file are set 4. Start these
   * ConfigNode by yourself 5. Add @Test 6. run ratisConsensusRegisterDemo 7. run
   * ratisConsensusQueryDemo
   */
  public void ratisConsensusSetStorageGroupsDemo() throws TException, InterruptedException {
    createClients();
    setStorageGroups();
  }

  public void ratisConsensusQueryDataNodesDemo() throws TException, InterruptedException {
    createClients();
    queryDataNodes();
  }

  public void ratisConsensusQueryStorageGroupsDemo() throws TException, InterruptedException {
    createClients();
    queryStorageGroups();
  }

  private void createClients() throws TTransportException {
    // Create clients for these three ConfigNodes
    // to simulate DataNodes to send RPC requests
    clients = new ConfigIService.Client[3];
    for (int i = 0; i < 3; i++) {
      TTransport transport =
          RpcTransportFactory.INSTANCE.getTransport(localhost, 22277 + i * 2, timeOutInMS);
      transport.open();
      clients[i] = new ConfigIService.Client(new TBinaryProtocol(transport));
    }
  }

  private void registerDataNodes() throws TException, InterruptedException {
    // DataNodes can connect to any ConfigNode and send write requests
    for (int i = 0; i < 10; i++) {
      EndPoint endPoint = new EndPoint("0.0.0.0", 6667 + i);
      TDataNodeRegisterReq req = new TDataNodeRegisterReq(endPoint);
      TDataNodeRegisterResp resp = clients[0].registerDataNode(req);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
      Assert.assertEquals(i, resp.getDataNodeID());
      System.out.printf(
          "\nRegister DataNode successful. DataNodeID: %d, %s\n", resp.getDataNodeID(), endPoint);

      TimeUnit.SECONDS.sleep(1);
    }
  }

  private void queryDataNodes() throws InterruptedException, TException {
    // sleep 1s to make sure all ConfigNode in ConfigNodeGroup hold the same PartitionTable
    TimeUnit.SECONDS.sleep(1);

    // DataNodes can connect to any ConfigNode and send read requests
    for (int i = 0; i < 3; i++) {
      TDataNodeMessageResp msgMap = clients[i].getDataNodesMessage(-1);
      System.out.printf(
          "\nQuery DataNode message from ConfigNode 0.0.0.0:%d. Result: %s\n",
          22277 + i * 2, msgMap);
    }
  }

  private void setStorageGroups() throws TException, InterruptedException {
    for (int i = 0; i < 10; i++) {
      TSetStorageGroupReq req = new TSetStorageGroupReq("root.sg" + i);
      clients[0].setStorageGroup(req);
      System.out.printf("\nSet StorageGroup successful. StorageGroup: %s\n", "root.sg" + i);
      TimeUnit.SECONDS.sleep(1);
    }
  }

  private void queryStorageGroups() throws InterruptedException, TException {
    // sleep 1s to make sure all ConfigNode in ConfigNodeGroup hold the same PartitionTable
    TimeUnit.SECONDS.sleep(1);

    for (int i = 0; i < 3; i++) {
      TStorageGroupMessageResp msgMap = clients[i].getStorageGroupsMessage();
      System.out.printf(
          "\nQuery StorageGroup message from ConfigNode 0.0.0.0:%d. Result: {\n", 22277 + i * 2);
      for (Map.Entry<String, TStorageGroupMessage> entry :
          msgMap.getStorageGroupMessageMap().entrySet()) {
        System.out.printf("  Key(%s)=%s\n", entry.getKey(), entry.getValue().toString());
      }
      System.out.println("}");
    }
  }
}
