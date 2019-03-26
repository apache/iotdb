package org.apache.iotdb.cluster.utils;

import static org.junit.Assert.*;

import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RouterTest {

  ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  String[] ipListOld;
  int portOld;
  int replicatorOld;

  @Before
  public void setUp() throws Exception {
    ipListOld = config.getNodes();
    portOld = config.getPort();
    replicatorOld = config.getReplication();

  }

  @After
  public void tearDown() throws Exception {
    config.setNodes(ipListOld);
    config.setPort(portOld);
    config.setReplication(replicatorOld);
  }

//	@Test
//	public void testRouteGroup() {
//
//	}

  @Test
  public void testGenerateGroups1() {
    String[] ipList = {"192.168.130.1", "192.168.130.2", "192.168.130.3", "192.168.130.4",
        "192.168.130.5",};
    int port = 7777;
    int replicator = 3;
    config.setNodes(ipList);
    config.setPort(port);
    config.setReplication(replicator);

    Router router = Router.getInstance();
    router.init();
//    router.showPhysicalRing();
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
      PhysicalNode[][] expected = generateNodesArray(ipIndex[i - 1], 3, 3, port);
      assertEquals(expected, router.generateGroups("192.168.130." + i, port));
    }
  }

  @Test
  public void testGenerateGroups2() {
    String[] ipList = {"192.168.130.1", "192.168.130.2", "192.168.130.3"};
    int port = 7777;
    int replicator = 3;
    config.setNodes(ipList);
    config.setPort(port);
    config.setReplication(replicator);

    Router router = Router.getInstance();
    router.init();
//    router.showPhysicalRing();
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
      PhysicalNode[][] expected = generateNodesArray(ipIndex[i - 1], 1, 3, port);
      assertEquals(expected, router.generateGroups("192.168.130." + i, port));
    }
  }

  boolean assertEquals(PhysicalNode[][] expect, PhysicalNode[][] actual) {
    if (expect.length != actual.length) {
      return false;
    }
    int len = expect.length;
    for (int i = 0; i < len; i++) {
      if (!assertEquals(expect[i], actual[i])) {
        return false;
      }
    }
    return true;
  }

  boolean assertEquals(PhysicalNode[] expect, PhysicalNode[] actual) {
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
