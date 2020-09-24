package org.apache.iotdb.cluster.client;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.ServerSocket;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.Test;

public class DataClientProviderTest {

  @Test
  public void testAsync() throws IOException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    DataClientProvider provider = new DataClientProvider(new Factory());

    assertNotNull(provider.getAsyncDataClient(TestUtils.getNode(0), 100));
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
  }

  @Test
  public void testSync() throws IOException, InterruptedException {
    Node node = new Node();
    node.setDataPort(9003).setIp("localhost");
    ServerSocket serverSocket = new ServerSocket(node.getDataPort());
    Thread listenThread = new Thread(() -> {
      while (!Thread.interrupted()) {
        try {
          serverSocket.accept();
        } catch (IOException e) {
          return;
        }
      }
    });
    listenThread.start();

    try {
      boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);
      DataClientProvider provider = new DataClientProvider(new Factory());

      assertNotNull(provider.getSyncDataClient(node, 100));
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    } finally {
      serverSocket.close();
      listenThread.interrupt();
      listenThread.join();
    }
  }
}