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

import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeMessage;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.SetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.StorageGroupMessage;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

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
   * ConfigNode by yourself 5. Add @Test and run
   */
  public void ratisConsensusDemo() throws TException, InterruptedException {
    createClients();

    registerDataNodes();
    queryDataNodes();

    setStorageGroups();
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

  private void registerDataNodes() throws TException {
    // DataNodes can connect to any ConfigNode and send write requests
    for (int i = 0; i < 3; i++) {
      DataNodeRegisterReq req = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6667 + i));
      DataNodeRegisterResp resp = clients[i].registerDataNode(req);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.registerResult.getCode());
      Assert.assertEquals(i, resp.getDataNodeID());
    }
  }

  private void queryDataNodes() throws InterruptedException, TException {
    // sleep 1s to make sure all ConfigNode in ConfigNodeGroup hold the same PartitionTable
    TimeUnit.SECONDS.sleep(1);

    // DataNodes can connect to any ConfigNode and send read requests
    for (int i = 0; i < 3; i++) {
      Map<Integer, DataNodeMessage> msgMap = clients[i].getDataNodesMessage(-1);
      Assert.assertEquals(3, msgMap.size());
      for (int j = 0; j < 3; j++) {
        Assert.assertNotNull(msgMap.get(j));
        Assert.assertEquals(j, msgMap.get(j).getDataNodeID());
        Assert.assertEquals(localhost, msgMap.get(j).getEndPoint().getIp());
        Assert.assertEquals(6667 + j, msgMap.get(j).getEndPoint().getPort());
      }
    }
  }

  private void setStorageGroups() throws TException {
    // DataNodes can connect to any ConfigNode and send write requests
    for (int i = 0; i < 3; i++) {
      SetStorageGroupReq req = new SetStorageGroupReq("root.sg" + i);
      TSStatus status = clients[i].setStorageGroup(req);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }

  private void queryStorageGroups() throws InterruptedException, TException {
    // sleep 1s to make sure all ConfigNode in ConfigNodeGroup hold the same PartitionTable
    TimeUnit.SECONDS.sleep(1);

    // DataNodes can connect to any ConfigNode and send read requests
    for (int i = 0; i < 3; i++) {
      Map<String, StorageGroupMessage> msgMap = clients[i].getStorageGroupsMessage();
      Assert.assertEquals(3, msgMap.size());
      for (int j = 0; j < 3; j++) {
        Assert.assertNotNull(msgMap.get("root.sg" + j));
        Assert.assertEquals("root.sg" + j, msgMap.get("root.sg" + j).getStorageGroup());
      }
    }
  }

  /**
   * This code tests the high availability of the ratis-consensus protocol. Make sure that you have
   * run according to the comments of ratisConsensusTest before executing this code. Next, kill
   * ConfigNode that occupies ports 22281 and 22282 on the local machine. Finally, run this test.
   */
  public void killDemo() throws TException {
    clients = new ConfigIService.Client[2];
    for (int i = 0; i < 2; i++) {
      TTransport transport =
          RpcTransportFactory.INSTANCE.getTransport(localhost, 22277 + i * 2, timeOutInMS);
      transport.open();
      clients[i] = new ConfigIService.Client(new TBinaryProtocol(transport));
    }

    // DataNodes can still send read/write requests when one of the three ConfigNode is killed.

    DataNodeRegisterResp resp =
        clients[1].registerDataNode(new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6670)));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.registerResult.getCode());
    Assert.assertEquals(3, resp.getDataNodeID());

    for (int i = 0; i < 2; i++) {
      Map<Integer, DataNodeMessage> msgMap = clients[i].getDataNodesMessage(-1);
      Assert.assertEquals(4, msgMap.size());
      for (int j = 0; j < 4; j++) {
        Assert.assertNotNull(msgMap.get(j));
        Assert.assertEquals(j, msgMap.get(j).getDataNodeID());
        Assert.assertEquals(localhost, msgMap.get(j).getEndPoint().getIp());
        Assert.assertEquals(6667 + j, msgMap.get(j).getEndPoint().getPort());
      }
    }
  }
}
