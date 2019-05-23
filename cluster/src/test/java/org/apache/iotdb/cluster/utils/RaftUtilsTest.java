/**
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
package org.apache.iotdb.cluster.utils;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.alipay.remoting.AsyncContext;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.alipay.sofa.jraft.entity.PeerId;
import java.util.Map;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.MetadataStateManchine;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.qp.task.QPTask;
import org.apache.iotdb.cluster.qp.task.SingleQPTask;
import org.apache.iotdb.cluster.rpc.raft.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.raft.request.nonquery.DataGroupNonQueryRequest;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class RaftUtilsTest {

  private ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  private String[] ipListOld;
  private int replicatorOld;
  private int numOfVirtualNodesOld;
  private int PORT = 7777;
  private String[] ipList = {
      "192.168.130.4:" + PORT,
      "192.168.130.5:" + PORT,
      "192.168.130.2:" + PORT,
      "192.168.130.1:" + PORT,
      "192.168.130.3:" + PORT
  };
  private int replicator = 3;

  private String[] storageGroups = {
      "root.d1",
      "root.d2",
      "root.d3",
      "root.d4",
      "root.d5"
  };

  @Mock
  private Server server;
  @Mock
  private MetadataRaftHolder metadataHolder;
  @Mock
  private RaftService service;
  @Mock
  private Node node;
  @Mock
  private QPTask qpTask;
  @Mock
  private BasicResponse response;
  @Mock
  private BasicRequest request;

  @Mock
  AsyncContext asyncContext;
  @Mock
  private SingleQPTask nullReadTask;

  @Mock
  Router router;
  @Mock
  MetadataStateManchine metadataStateManchine;

  private List<PeerId> peerIds;

  @Before
  public void setUp() throws Exception {
    peerIds = new ArrayList<>();
    for (String addr : ipList) {
      peerIds.add(PeerId.parsePeer(addr));
    }
    MockitoAnnotations.initMocks(this);
    when(server.getMetadataHolder()).thenReturn(metadataHolder);
    when(metadataHolder.getService()).thenReturn(service);
    when(service.getPeerIdList()).thenReturn(peerIds);
    when(service.getNode()).thenReturn(node);
    when((service).getFsm()).thenReturn(metadataStateManchine);
    when(metadataStateManchine.getAllStorageGroups()).thenReturn(new HashSet<>(Arrays.asList(storageGroups)));
    Mockito.doNothing().when(node).apply(any(Task.class));
    Mockito.doNothing().when(response).addResult(any(boolean.class));
    Mockito.doNothing().when(response).setErrorMsg(any(String.class));
    ipListOld = config.getNodes();
    replicatorOld = config.getReplication();
    numOfVirtualNodesOld = config.getNumOfVirtualNodes();

    int numOfVirtualNodes = 2;
    config.setPort(PORT);
    config.setNodes(ipList);
    config.setReplication(replicator);
    config.setNumOfVirtualNodes(numOfVirtualNodes);
    router = Router.getInstance();
    router.init(config);
    router.showPhysicalRing();
  }

  @After
  public void tearDown() throws Exception {
    peerIds.clear();
    config.setNodes(ipListOld);
    config.setReplication(replicatorOld);
    config.setNumOfVirtualNodes(numOfVirtualNodesOld);
  }

  @Test
  public void testGetLeaderPeerID() {
    RaftUtils.clearRaftGroupLeader();
    PeerId metadtaLeader = PeerId.parsePeer(ipList[0]);
    RaftUtils.updateRaftGroupLeader(ClusterConfig.METADATA_GROUP_ID, metadtaLeader);
    assertEquals(metadtaLeader, RaftUtils.getLocalLeaderPeerID(ClusterConfig.METADATA_GROUP_ID));

    boolean[] isLeaderCached = {true, false, true, false, true};
    for (int i = 0; i < ipList.length; i++) {
      if (isLeaderCached[i]) {
        PeerId leaderExpeted = PeerId.parsePeer(ipList[(i + 1) % ipList.length]);
        RaftUtils.updateRaftGroupLeader(Router.DATA_GROUP_STR + i, leaderExpeted);
        PeerId leaderActual = RaftUtils.getLocalLeaderPeerID(Router.DATA_GROUP_STR + i);
        assertTrue(leaderExpeted.equals(leaderActual));

      } else {
        PeerId leader = RaftUtils.getLocalLeaderPeerID(Router.DATA_GROUP_STR + i);
        boolean flag = false;
        for (int j = 0; j < replicator; j++) {
          String addr = ipList[(i + j) % ipList.length];
          if (leader.equals(PeerId.parsePeer(addr))) {
            flag = true;
            break;
          }
        }
        assertTrue(flag);
      }
    }
    RaftUtils.clearRaftGroupLeader();
  }

  @Test
  public void testGetRandomPeerID() {
    Router router = Router.getInstance();
    for (int i = 0; i < 100; i++) {
      PeerId id = RaftUtils.getRandomPeerID(ClusterConfig.METADATA_GROUP_ID, server, router);
      assertTrue(peerIds.contains(id));
    }

    for (int i = 0; i < 100; i++) {
      int groudID = i % ipList.length;
      PeerId id = RaftUtils.getRandomPeerID(Router.DATA_GROUP_STR + groudID, server, router);
      boolean flag = false;
      for (int j = 0; j < replicator; j++) {
        String addr = ipList[(groudID + j) % ipList.length];
        if (id.equals(PeerId.parsePeer(addr))) {
          flag = true;
          break;
        }
      }
      assertTrue(flag);
    }
  }

  @Test
  public void testGetPeerIDFrom() {
    PhysicalNode node = new PhysicalNode("1.2.3.4", 1234);
    PeerId id = new PeerId("1.2.3.4", 1234);
    assertEquals(id, RaftUtils.getPeerIDFrom(node));
  }

  @Test
  public void testGetPhysicalNodeFrom() {
    PeerId id = new PeerId("1.2.3.4", 1234);
    PhysicalNode node = new PhysicalNode("1.2.3.4", 1234);
    assertEquals(node, RaftUtils.getPhysicalNodeFrom(id));
  }

  @Test
  public void testConvertStringArrayToPeerIdArray() {
    PeerId[] peerIds = new PeerId[ipList.length];
    for (int i = 0; i < ipList.length; i++) {
      peerIds[i] = PeerId.parsePeer(ipList[i]);
    }
    assertArrayEquals(peerIds, RaftUtils.convertStringArrayToPeerIdArray(ipList));
  }

  @Test
  public void testGetPhysicalNodeAndPeerIdArrayFrom() {
    PhysicalNode[] pNodes = new PhysicalNode[ipList.length];
    PeerId[] peerIds = new PeerId[ipList.length];
    for (int i = 0; i < ipList.length; i++) {
      String[] values = ipList[i].split(":");
      pNodes[i] = new PhysicalNode(values[0], Integer.parseInt(values[1]));
      peerIds[i] = PeerId.parsePeer(ipList[i]);
    }
    assertArrayEquals(pNodes, RaftUtils.getPhysicalNodeArrayFrom(peerIds));
    assertArrayEquals(peerIds, RaftUtils.getPeerIdArrayFrom(pNodes));
  }

  @Test
  public void testExecuteRaftTaskForLocalProcessor() throws InterruptedException, IOException {
    DataGroupNonQueryRequest request = new DataGroupNonQueryRequest("", new ArrayList<>());
    Mockito.doNothing().when(qpTask).await();
    when(qpTask.getRequest()).thenReturn(request);
    when(qpTask.getResponse()).thenReturn(null);
    assertFalse(RaftUtils.executeRaftTaskForLocalProcessor(service, qpTask, response));
  }

  @Test
  public void testExecuteRaftTaskForRpcProcessor() throws IOException {
    DataGroupNonQueryRequest request = new DataGroupNonQueryRequest("", new ArrayList<>());
    RaftUtils.executeRaftTaskForRpcProcessor(service, asyncContext, request, response);
  }

  @Test
  public void testHandleNullReadToMetaGroup() throws InterruptedException {
    Mockito.doNothing().when(nullReadTask).await();
    RaftUtils.handleNullReadToMetaGroup(new Status(), server, nullReadTask);
  }

  @Test
  public void testGetDataPartitionOfSG() {
    PeerId metadtaLeader = PeerId.parsePeer(ipList[0]);
    RaftUtils.updateRaftGroupLeader(ClusterConfig.METADATA_GROUP_ID, metadtaLeader);
    PeerId[] metadataGroup = RaftUtils.getDataPartitionOfSG(null, server, router);
    assertArrayEquals(peerIds.toArray(new PeerId[peerIds.size()]), metadataGroup);

    String sg = storageGroups[0];
    PhysicalNode[] physicalGroup = router.routeGroup(sg);
    String groupId = router.getGroupID(physicalGroup);
    PeerId[] expectGroup = RaftUtils.getPeerIdArrayFrom(physicalGroup);
    RaftUtils.updateRaftGroupLeader(groupId, expectGroup[0]);
    PeerId[] dataGroup = RaftUtils.getDataPartitionOfSG(sg, server, router);
    assertArrayEquals(expectGroup, dataGroup);
  }

  @Test
  public void testGetDataPartitionOfNode() {
    PeerId node = peerIds.get(0);
    Map<String[], String[]> dataPartition = RaftUtils.getDataPartitionOfNode(node.getIp(), node.getPort(), server, router);
    Map<String, String[]> dataPartitionMap = new HashMap<>();
    dataPartition.forEach((key, value) -> dataPartitionMap.put(buildKey(key), value));

    String[][] keys = {{"192.168.130.1", "192.168.130.3", "192.168.130.4"},
        {"192.168.130.3", "192.168.130.4", "192.168.130.5"},
        {"192.168.130.2", "192.168.130.4", "192.168.130.5"}};
    String[][] values = {{},
        {"root.d1", "root.d4"},
        {"root.d3", "root.d2"}};
    Map<String, String[]> expectMap = new HashMap<>();
    for (int i = 0; i < keys.length; i++) {
      expectMap.put(buildKey(keys[i]), values[i]);
    }

    assertEquals(dataPartitionMap.size(), expectMap.size());
    dataPartitionMap.forEach((key, value) -> assertArrayEquals(value, expectMap.get(key)));
  }

  private String buildKey(String[] ss) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < ss.length; i++) {
      builder.append(ss[i]).append('#');
    }
    return builder.toString();
  }
}
