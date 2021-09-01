package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.ClientUtils;

import java.io.IOException;
import java.net.ServerSocket;

public class BaseClientTest {

  protected Node defaultNode = constructDefaultNode();

  private ServerSocket metaServer;
  private Thread metaServerListeningThread;

  private ServerSocket dataServer;
  private Thread dataServerListeningThread;

  private ServerSocket metaHeartbeatServer;
  private Thread metaHeartbeatServerListeningThread;

  private ServerSocket dataHeartbeatServer;
  private Thread dataHeartbeatServerListeningThread;

  public void startMetaServer() throws IOException {
    metaServer = new ServerSocket(ClientUtils.getPort(defaultNode, ClientCategory.META));
    metaServerListeningThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  metaServer.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    metaServerListeningThread.start();
  }

  public void startDataServer() throws IOException {
    dataServer = new ServerSocket(ClientUtils.getPort(defaultNode, ClientCategory.DATA));
    dataServerListeningThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  dataServer.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    dataServerListeningThread.start();
  }

  public void startMetaHeartbeatServer() throws IOException {
    metaHeartbeatServer =
        new ServerSocket(ClientUtils.getPort(defaultNode, ClientCategory.META_HEARTBEAT));
    metaHeartbeatServerListeningThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  metaHeartbeatServer.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    metaHeartbeatServerListeningThread.start();
  }

  public void startDataHeartbeatServer() throws IOException {
    dataHeartbeatServer =
        new ServerSocket(ClientUtils.getPort(defaultNode, ClientCategory.DATA_HEARTBEAT));
    dataHeartbeatServerListeningThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  dataHeartbeatServer.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    dataHeartbeatServerListeningThread.start();
  }

  public void stopMetaServer() throws InterruptedException, IOException {
    if (metaServer != null) {
      metaServer.close();
    }
    if (metaServerListeningThread != null) {
      metaServerListeningThread.interrupt();
      metaServerListeningThread.join();
    }
  }

  public void stopDataServer() throws IOException, InterruptedException {
    if (dataServer != null) {
      dataServer.close();
    }
    if (dataServerListeningThread != null) {
      dataServerListeningThread.interrupt();
      dataServerListeningThread.join();
    }
  }

  public void stopMetaHeartbeatServer() throws IOException, InterruptedException {
    if (metaHeartbeatServer != null) {
      metaHeartbeatServer.close();
    }
    if (metaHeartbeatServerListeningThread != null) {
      metaHeartbeatServerListeningThread.interrupt();
      metaHeartbeatServerListeningThread.join();
    }
  }

  public void stopDataHeartbeatServer() throws IOException, InterruptedException {
    if (dataHeartbeatServer != null) {
      dataHeartbeatServer.close();
    }
    if (dataServerListeningThread != null) {
      dataServerListeningThread.interrupt();
      dataServerListeningThread.join();
    }
  }

  public Node constructDefaultNode() {
    Node node = new Node();
    node.setMetaPort(9003).setInternalIp("localhost").setClientIp("localhost");
    node.setDataPort(40010);
    return node;
  }
}
