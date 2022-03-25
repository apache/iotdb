package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeMessage;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterResp;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ConfigManagerTest {

  private static final String localhost = "0.0.0.0";
  private static final int timeOutInMS = 100;

  private ConfigIService.Client[] clients;

  /**
   * This is a temporary test of ConfigNode's integration with the ratis-consensus protocol To run
   * this code, follow these steps: 1. Compile IoTDB 2. Copy at least three
   * iotdb-confignode-0.14.0-SNAPSHOT 3. Make sure these parameters: config_node_rpc_address(all
   * 0.0.0.0), config_node_rpc_port(22277, 22279, 22281), config_node_internal_port(22278, 22280,
   * 22282), consensus_type(all ratis) and config_node_group_address_list(all
   * 0.0.0.0:22278,0.0.0.0:22280,0.0.0.0:22282) in each iotdb-confignode.properties file are set 4.
   * Start these ConfigNode by yourself 5. Add @Test and run
   */
  @Test
  public void ratisConsensusTest() throws TException, InterruptedException {
    createClients();

    registerDataNodes();

    queryDataNodes();
  }

  private void createClients() throws TTransportException {
    clients = new ConfigIService.Client[3];
    for (int i = 0; i < 3; i++) {
      TTransport transport =
          RpcTransportFactory.INSTANCE.getTransport(localhost, 22277 + i * 2, timeOutInMS);
      transport.open();
      clients[i] = new ConfigIService.Client(new TBinaryProtocol(transport));
    }
  }

  private void registerDataNodes() throws TException {
    for (int i = 0; i < 3; i++) {
      DataNodeRegisterReq req = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6667 + i));
      DataNodeRegisterResp resp = clients[0].registerDataNode(req);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.registerResult.getCode());
      Assert.assertEquals(i, resp.getDataNodeID());
    }
  }

  private void queryDataNodes() throws InterruptedException, TException {
    // sleep 100ms to make sure all ConfigNode in ConfigNodeGroup hold the same PartitionTable
    TimeUnit.MILLISECONDS.sleep(100);

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
}
