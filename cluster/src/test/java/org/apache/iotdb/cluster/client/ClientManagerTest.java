package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ClientManagerTest extends BaseClientTest {

  @Before
  public void setUp() throws IOException {
    startDataServer();
    startMetaServer();
    startDataHeartbeatServer();
    startMetaHeartbeatServer();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    stopDataServer();
    stopMetaServer();
    stopDataHeartbeatServer();
    stopMetaHeartbeatServer();
  }

  @Test
  public void syncClientManagersTest() {
    // ---------Sync cluster clients manager test------------
    ClientManager clusterManager = new ClientManager(false, ClientManager.Type.ClusterClient);
    RaftService.Client syncClusterClient =
        clusterManager.borrowSyncClient(defaultNode, ClientCategory.DATA);

    Assert.assertNotNull(syncClusterClient);
    Assert.assertTrue(syncClusterClient instanceof SyncDataClient);
    Assert.assertEquals(((SyncDataClient) syncClusterClient).getNode(), defaultNode);
    Assert.assertTrue(syncClusterClient.getInputProtocol().getTransport().isOpen());
    ((SyncDataClient) syncClusterClient).returnSelf();

    // cluster test
    Assert.assertNull(clusterManager.borrowSyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));
    Assert.assertNull(clusterManager.borrowSyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(clusterManager.borrowSyncClient(defaultNode, ClientCategory.META_HEARTBEAT));

    Assert.assertNull(clusterManager.borrowAsyncClient(defaultNode, ClientCategory.DATA));
    Assert.assertNull(clusterManager.borrowAsyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));
    Assert.assertNull(clusterManager.borrowAsyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(clusterManager.borrowAsyncClient(defaultNode, ClientCategory.META_HEARTBEAT));

    // ---------Sync meta(meta heartbeat) clients manager test------------
    ClientManager metaManager = new ClientManager(false, ClientManager.Type.MetaGroupClient);
    RaftService.Client metaClient = metaManager.borrowSyncClient(defaultNode, ClientCategory.META);
    Assert.assertNotNull(metaClient);
    Assert.assertTrue(metaClient instanceof SyncMetaClient);
    Assert.assertEquals(((SyncMetaClient) metaClient).getNode(), defaultNode);
    Assert.assertTrue(metaClient.getInputProtocol().getTransport().isOpen());
    ((SyncMetaClient) metaClient).returnSelf();

    RaftService.Client metaHeartClient =
        metaManager.borrowSyncClient(defaultNode, ClientCategory.META_HEARTBEAT);
    Assert.assertNotNull(metaHeartClient);
    Assert.assertTrue(metaHeartClient instanceof SyncMetaClient);
    Assert.assertEquals(((SyncMetaClient) metaHeartClient).getNode(), defaultNode);
    Assert.assertTrue(metaHeartClient.getInputProtocol().getTransport().isOpen());
    ((SyncMetaClient) metaHeartClient).returnSelf();

    // cluster test
    Assert.assertNull(metaManager.borrowSyncClient(defaultNode, ClientCategory.DATA));
    Assert.assertNull(metaManager.borrowSyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));

    Assert.assertNull(metaManager.borrowAsyncClient(defaultNode, ClientCategory.DATA));
    Assert.assertNull(metaManager.borrowAsyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));
    Assert.assertNull(metaManager.borrowAsyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(metaManager.borrowAsyncClient(defaultNode, ClientCategory.META_HEARTBEAT));

    // ---------Sync data(data heartbeat) clients manager test------------
    ClientManager dataManager = new ClientManager(false, ClientManager.Type.DataGroupClient);

    RaftService.Client dataClient = dataManager.borrowSyncClient(defaultNode, ClientCategory.DATA);
    Assert.assertNotNull(dataClient);
    Assert.assertTrue(dataClient instanceof SyncDataClient);
    Assert.assertEquals(((SyncDataClient) dataClient).getNode(), defaultNode);
    Assert.assertTrue(dataClient.getInputProtocol().getTransport().isOpen());
    ((SyncDataClient) dataClient).returnSelf();

    RaftService.Client dataHeartClient =
        dataManager.borrowSyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT);
    Assert.assertNotNull(dataHeartClient);
    Assert.assertTrue(dataHeartClient instanceof SyncDataClient);
    Assert.assertEquals(((SyncDataClient) dataHeartClient).getNode(), defaultNode);
    Assert.assertTrue(dataHeartClient.getInputProtocol().getTransport().isOpen());
    ((SyncDataClient) dataHeartClient).returnSelf();

    // cluster test
    Assert.assertNull(dataManager.borrowSyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(dataManager.borrowSyncClient(defaultNode, ClientCategory.META_HEARTBEAT));

    Assert.assertNull(dataManager.borrowAsyncClient(defaultNode, ClientCategory.DATA));
    Assert.assertNull(dataManager.borrowAsyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));
    Assert.assertNull(dataManager.borrowAsyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(dataManager.borrowAsyncClient(defaultNode, ClientCategory.META_HEARTBEAT));
  }

  @Test
  public void asyncClientManagersTest() {
    // ---------async cluster clients manager test------------
    ClientManager clusterManager = new ClientManager(true, ClientManager.Type.ClusterClient);
    RaftService.AsyncClient clusterClient =
        clusterManager.borrowAsyncClient(defaultNode, ClientCategory.DATA);

    Assert.assertNotNull(clusterClient);
    Assert.assertTrue(clusterClient instanceof AsyncDataClient);
    Assert.assertEquals(((AsyncDataClient) clusterClient).getNode(), defaultNode);
    Assert.assertTrue(((AsyncDataClient) clusterClient).isValid());
    Assert.assertTrue(((AsyncDataClient) clusterClient).isReady());
    ((AsyncDataClient) clusterClient).returnSelf();
    clusterClient = null;

    // cluster test
    Assert.assertNull(clusterManager.borrowAsyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));
    Assert.assertNull(clusterManager.borrowAsyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(clusterManager.borrowAsyncClient(defaultNode, ClientCategory.META_HEARTBEAT));

    Assert.assertNull(clusterManager.borrowSyncClient(defaultNode, ClientCategory.DATA));
    Assert.assertNull(clusterManager.borrowSyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));
    Assert.assertNull(clusterManager.borrowSyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(clusterManager.borrowSyncClient(defaultNode, ClientCategory.META_HEARTBEAT));

    // ---------async meta(meta heartbeat) clients manager test------------
    ClientManager metaManager = new ClientManager(true, ClientManager.Type.MetaGroupClient);
    RaftService.AsyncClient metaClient =
        metaManager.borrowAsyncClient(defaultNode, ClientCategory.META);
    Assert.assertNotNull(metaClient);
    Assert.assertTrue(metaClient instanceof AsyncMetaClient);
    Assert.assertEquals(((AsyncMetaClient) metaClient).getNode(), defaultNode);
    Assert.assertTrue(((AsyncMetaClient) metaClient).isValid());
    Assert.assertTrue(((AsyncMetaClient) metaClient).isReady());
    ((AsyncMetaClient) metaClient).returnSelf();
    metaClient = null;

    RaftService.AsyncClient metaHeartClient =
        metaManager.borrowAsyncClient(defaultNode, ClientCategory.META_HEARTBEAT);
    Assert.assertNotNull(metaHeartClient);
    Assert.assertTrue(metaHeartClient instanceof AsyncMetaClient);
    Assert.assertEquals(((AsyncMetaClient) metaHeartClient).getNode(), defaultNode);
    Assert.assertTrue(((AsyncMetaClient) metaHeartClient).isValid());
    Assert.assertTrue(((AsyncMetaClient) metaHeartClient).isReady());
    ((AsyncMetaClient) metaHeartClient).returnSelf();
    metaHeartClient = null;

    // cluster test
    Assert.assertNull(metaManager.borrowAsyncClient(defaultNode, ClientCategory.DATA));
    Assert.assertNull(metaManager.borrowAsyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));

    Assert.assertNull(metaManager.borrowSyncClient(defaultNode, ClientCategory.DATA));
    Assert.assertNull(metaManager.borrowSyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));
    Assert.assertNull(metaManager.borrowSyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(metaManager.borrowSyncClient(defaultNode, ClientCategory.META_HEARTBEAT));

    // ---------async data(data heartbeat) clients manager test------------
    ClientManager dataManager = new ClientManager(true, ClientManager.Type.DataGroupClient);

    RaftService.AsyncClient dataClient =
        dataManager.borrowAsyncClient(defaultNode, ClientCategory.DATA);
    Assert.assertNotNull(dataClient);
    Assert.assertTrue(dataClient instanceof AsyncDataClient);
    Assert.assertEquals(((AsyncDataClient) dataClient).getNode(), defaultNode);
    Assert.assertTrue(((AsyncDataClient) dataClient).isValid());
    Assert.assertTrue(((AsyncDataClient) dataClient).isReady());
    ((AsyncDataClient) dataClient).returnSelf();
    dataClient = null;

    RaftService.AsyncClient dataHeartClient =
        dataManager.borrowAsyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT);
    Assert.assertNotNull(dataHeartClient);
    Assert.assertTrue(dataHeartClient instanceof AsyncDataClient);
    Assert.assertEquals(((AsyncDataClient) dataHeartClient).getNode(), defaultNode);
    Assert.assertTrue(((AsyncDataClient) dataHeartClient).isValid());
    Assert.assertTrue(((AsyncDataClient) dataHeartClient).isReady());
    ((AsyncDataClient) dataHeartClient).returnSelf();
    dataHeartClient = null;

    // cluster test
    Assert.assertNull(dataManager.borrowAsyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(dataManager.borrowAsyncClient(defaultNode, ClientCategory.META_HEARTBEAT));

    Assert.assertNull(dataManager.borrowSyncClient(defaultNode, ClientCategory.DATA));
    Assert.assertNull(dataManager.borrowSyncClient(defaultNode, ClientCategory.DATA_HEARTBEAT));
    Assert.assertNull(dataManager.borrowSyncClient(defaultNode, ClientCategory.META));
    Assert.assertNull(dataManager.borrowSyncClient(defaultNode, ClientCategory.META_HEARTBEAT));
  }
}
