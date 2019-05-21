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
package org.apache.iotdb.cluster.utils.hash;

import static org.junit.Assert.*;

import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RouterTest {

  private ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  private String[] ipListOld;
  private int replicatorOld;
  private int numOfVirtualNodesOld;
//  private HashFunction function = new MD5Hash();
  private final int PORT = 7777;

  @Before
  public void setUp() throws Exception {
    ipListOld = config.getNodes();
    replicatorOld = config.getReplication();
    numOfVirtualNodesOld = config.getNumOfVirtualNodes();
  }

  @After
  public void tearDown() throws Exception {
    config.setNodes(ipListOld);
    config.setReplication(replicatorOld);
    config.setNumOfVirtualNodes(numOfVirtualNodesOld);
    Router.getInstance().init();
  }

  @Test
  public void testRouteNodeAndGroup1() {
    String[] ipList = {"192.168.130.1:" + PORT, "192.168.130.2:" + PORT, "192.168.130.3:" + PORT,
        "192.168.130.4:" + PORT,
        "192.168.130.5:" + PORT};
    int replicator = 3;
    int numOfVirtulaNodes = 2;
    config.setNodes(ipList);
    config.setReplication(replicator);
    config.setNumOfVirtualNodes(numOfVirtulaNodes);
    Router router = Router.getInstance();
    router.init();
    String sg1 = "root.device.sensor";
    assertTrue(router.routeNode(sg1).equals(new PhysicalNode("192.168.130.4", PORT)));
    PhysicalNode[] expected1 = {
        new PhysicalNode("192.168.130.4", PORT),
        new PhysicalNode("192.168.130.5", PORT),
        new PhysicalNode("192.168.130.2", PORT)
    };
    assertPhysicalNodeEquals(expected1, router.routeGroup(sg1));
    // test cache
    assertPhysicalNodeEquals(expected1, router.routeGroup(sg1));
    assertEquals(Router.DATA_GROUP_STR + "0", router.getGroupID(router.routeGroup(sg1)));

    String sg2 = "root.device.sensor2";
    assertTrue(router.routeNode(sg2).equals(new PhysicalNode("192.168.130.3", PORT)));
    PhysicalNode[] expected2 = {
        new PhysicalNode("192.168.130.3", PORT),
        new PhysicalNode("192.168.130.4", PORT),
        new PhysicalNode("192.168.130.5", PORT)
    };
    assertPhysicalNodeEquals(expected2, router.routeGroup(sg2));
    // test cache
    assertPhysicalNodeEquals(expected2, router.routeGroup(sg2));
    assertEquals(Router.DATA_GROUP_STR + "4", router.getGroupID(router.routeGroup(sg2)));
  }

  @Test
  public void testRouteNodeAndGroup2() {
    String[] ipList = {"192.168.130.1:" + PORT, "192.168.130.2:" + PORT, "192.168.130.3:" + PORT};
    int replicator = 3;
    int numOfVirtulaNodes = 2;

    config.setNodes(ipList);
    config.setReplication(replicator);
    config.setNumOfVirtualNodes(numOfVirtulaNodes);

    Router router = Router.getInstance();
    router.init();
    String sg1 = "root.device.sensor";
    assertEquals(router.routeNode(sg1), new PhysicalNode("192.168.130.3", PORT));
    PhysicalNode[] expected1 = {
        new PhysicalNode("192.168.130.3", PORT),
        new PhysicalNode("192.168.130.2", PORT),
        new PhysicalNode("192.168.130.1", PORT)
    };
    assertPhysicalNodeEquals(expected1, router.routeGroup(sg1));
    // test cache
    assertPhysicalNodeEquals(expected1, router.routeGroup(sg1));
    assertEquals(Router.DATA_GROUP_STR + "0", router.getGroupID(router.routeGroup(sg1)));

    String sg2 = "root.vehicle.d1";
    assertEquals(router.routeNode(sg2), new PhysicalNode("192.168.130.2", PORT));
    PhysicalNode[] expected2 = {
        new PhysicalNode("192.168.130.2", PORT),
        new PhysicalNode("192.168.130.1", PORT),
        new PhysicalNode("192.168.130.3", PORT)
    };
    assertPhysicalNodeEquals(expected2, router.routeGroup(sg2));
    // test cache
    assertPhysicalNodeEquals(expected2, router.routeGroup(sg2));
    assertEquals(Router.DATA_GROUP_STR + "0", router.getGroupID(router.routeGroup(sg2)));
  }


  @Test
  public void testGenerateGroups1() {
    String[] ipList = {"192.168.130.1:" + PORT, "192.168.130.2:" + PORT, "192.168.130.3:" + PORT,
        "192.168.130.4:" + PORT,
        "192.168.130.5:" + PORT,};
    int replicator = 3;
    int numOfVirtulaNodes = 2;

    config.setNodes(ipList);
    config.setReplication(replicator);
    config.setNumOfVirtualNodes(numOfVirtulaNodes);

    Router router = Router.getInstance();
    router.init();
    String[][][] ipIndex = {
        {
            {"192.168.130.1", "192.168.130.3", "192.168.130.4",},
            {"192.168.130.2", "192.168.130.1", "192.168.130.3",},
            {"192.168.130.5", "192.168.130.2", "192.168.130.1",},
        },
        {
            {"192.168.130.2", "192.168.130.1", "192.168.130.3",},
            {"192.168.130.5", "192.168.130.2", "192.168.130.1",},
            {"192.168.130.4", "192.168.130.5", "192.168.130.2",},
        },
        {
            {"192.168.130.3", "192.168.130.4", "192.168.130.5",},
            {"192.168.130.1", "192.168.130.3", "192.168.130.4",},
            {"192.168.130.2", "192.168.130.1", "192.168.130.3",},
        },
        {
            {"192.168.130.4", "192.168.130.5", "192.168.130.2",},
            {"192.168.130.3", "192.168.130.4", "192.168.130.5",},
            {"192.168.130.1", "192.168.130.3", "192.168.130.4",},
        },
        {
            {"192.168.130.5", "192.168.130.2", "192.168.130.1",},
            {"192.168.130.4", "192.168.130.5", "192.168.130.2",},
            {"192.168.130.3", "192.168.130.4", "192.168.130.5",},
        },
    };
    for (int i = 1; i < 5; i++) {
      PhysicalNode[][] expected = generateNodesArray(ipIndex[i - 1], 3, 3, PORT);
      assertPhysicalNodeEquals(expected, router.getGroupsNodes("192.168.130." + i, PORT));
    }
  }

  @Test
  public void testGenerateGroups2() {
    String[] ipList = {"192.168.130.1:" + PORT, "192.168.130.2:" + PORT, "192.168.130.3:" + PORT};
    int replicator = 3;
    int numOfVirtulaNodes = 2;

    config.setNodes(ipList);
    config.setReplication(replicator);
    config.setNumOfVirtualNodes(numOfVirtulaNodes);

    Router router = Router.getInstance();
    router.init();
    String[][][] ipIndex = {
        {
            {"192.168.130.1", "192.168.130.3", "192.168.130.2",},
        },
        {
            {"192.168.130.2", "192.168.130.1", "192.168.130.3",},
        },
        {
            {"192.168.130.3", "192.168.130.2", "192.168.130.1",},
        },
    };
    for (int i = 1; i < 4; i++) {
      PhysicalNode[][] expected = generateNodesArray(ipIndex[i - 1], 1, 3, PORT);
      assertPhysicalNodeEquals(expected, router.getGroupsNodes("192.168.130." + i, PORT));
    }
  }

  boolean assertPhysicalNodeEquals(PhysicalNode[][] expect, PhysicalNode[][] actual) {
    if (expect.length != actual.length) {
      return false;
    }
    int len = expect.length;
    for (int i = 0; i < len; i++) {
      if (!assertPhysicalNodeEquals(expect[i], actual[i])) {
        return false;
      }
    }
    return true;
  }

  boolean assertPhysicalNodeEquals(PhysicalNode[] expect, PhysicalNode[] actual) {
    if (expect.length != actual.length) {
      return false;
    }
    int len = expect.length;
    for (int i = 0; i < len; i++) {
      if (!expect[i].equals(actual[i])) {
        return false;
      }
    }
    return true;
  }

  PhysicalNode[][] generateNodesArray(String[][] ip, int row, int col, int port) {
    PhysicalNode[][] result = new PhysicalNode[row][col];
    for (int i = 0; i < row; i++) {
      for (int j = 0; j < col; j++) {
        result[i][j] = new PhysicalNode(ip[i][j], port);
      }
    }
    return result;
  }
}
