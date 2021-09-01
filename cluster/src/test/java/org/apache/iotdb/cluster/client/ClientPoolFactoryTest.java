package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.utils.ClientUtils;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

public class ClientPoolFactoryTest {
  private ClusterConfig clusterConfig = ClusterDescriptor.getInstance().getConfig();
  private int maxClientPerNodePerMember;
  private long waitClientTimeoutMS;

  @Before
  public void setUp() {
    maxClientPerNodePerMember = clusterConfig.getMaxClientPerNodePerMember();
    waitClientTimeoutMS = clusterConfig.getWaitClientTimeoutMS();
    clusterConfig.setMaxClientPerNodePerMember(2);
    clusterConfig.setWaitClientTimeoutMS(10L);
  }

  @After
  public void tearDown() {
    clusterConfig.setMaxClientPerNodePerMember(maxClientPerNodePerMember);
    clusterConfig.setWaitClientTimeoutMS(waitClientTimeoutMS);
  }

  @Test
  public void createAsyncDataClientTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.AsyncClient> pool =
        ClientPoolFactory.getInstance().createAsyncDataPool(ClientCategory.DATA);

    Assert.assertEquals(pool.getMaxTotalPerKey(), 2);
    Assert.assertEquals(pool.getMaxWaitDuration().getNano(), 10 * 1000000);

    Node node = constructDefaultNode();

    RaftService.AsyncClient asyncClient = null;
    try {
      asyncClient = pool.borrowObject(node);

      Assert.assertNotNull(asyncClient);
      Assert.assertTrue(asyncClient instanceof AsyncDataClient);

    } finally {
      ((AsyncDataClient) asyncClient).returnSelf();
    }
  }

  @Test
  public void createAsyncMetaClientTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.AsyncClient> pool =
        ClientPoolFactory.getInstance().createAsyncDataPool(ClientCategory.META);

    Assert.assertEquals(pool.getMaxTotalPerKey(), 2);
    Assert.assertEquals(pool.getMaxWaitDuration().getNano(), 10 * 1000000);

    Node node = constructDefaultNode();

    RaftService.AsyncClient asyncClient = null;
    try {
      asyncClient = pool.borrowObject(node);

      Assert.assertNotNull(asyncClient);
      Assert.assertTrue(asyncClient instanceof AsyncMetaClient);

    } finally {
      ((AsyncMetaClient) asyncClient).returnSelf();
    }
  }

  @Test
  public void createSyncDataClientTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.Client> pool =
        ClientPoolFactory.getInstance().createSyncDataPool(ClientCategory.DATA_HEARTBEAT);

    Assert.assertEquals(pool.getMaxTotalPerKey(), 2);
    Assert.assertEquals(pool.getMaxWaitDuration().getNano(), 10 * 1000000);

    Node node = constructDefaultNode();

    RaftService.Client client = null;
    ServerSocket serverSocket =
        new ServerSocket(ClientUtils.getPort(node, ClientCategory.DATA_HEARTBEAT));
    Thread listenThread = null;
    try {
      listenThread =
          new Thread(
              () -> {
                while (!Thread.interrupted()) {
                  try {
                    serverSocket.accept();
                  } catch (IOException e) {
                    return;
                  }
                }
              });
      listenThread.start();

      client = pool.borrowObject(node);

      Assert.assertNotNull(client);
      Assert.assertTrue(client instanceof SyncDataClient);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      ((SyncDataClient) client).returnSelf();
      if (serverSocket != null) {
        serverSocket.close();
        listenThread.interrupt();
        listenThread.join();
      }
    }
  }

  @Test
  public void createSyncMetaClientTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.Client> pool =
        ClientPoolFactory.getInstance().createSyncMetaPool(ClientCategory.META_HEARTBEAT);

    Assert.assertEquals(pool.getMaxTotalPerKey(), 2);
    Assert.assertEquals(pool.getMaxWaitDuration().getNano(), 10 * 1000000);

    Node node = constructDefaultNode();

    RaftService.Client client = null;
    ServerSocket serverSocket =
        new ServerSocket(ClientUtils.getPort(node, ClientCategory.META_HEARTBEAT));
    Thread listenThread = null;
    try {
      listenThread =
          new Thread(
              () -> {
                while (!Thread.interrupted()) {
                  try {
                    serverSocket.accept();
                  } catch (IOException e) {
                    return;
                  }
                }
              });
      listenThread.start();

      client = pool.borrowObject(node);

      Assert.assertNotNull(client);
      Assert.assertTrue(client instanceof SyncMetaClient);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      ((SyncMetaClient) client).returnSelf();
      if (serverSocket != null) {
        serverSocket.close();
        listenThread.interrupt();
        listenThread.join();
      }
    }
  }

  private Node constructDefaultNode() {
    Node node = new Node();
    node.setMetaPort(9003).setInternalIp("localhost").setClientIp("localhost");
    node.setDataPort(40010);
    return node;
  }
}
