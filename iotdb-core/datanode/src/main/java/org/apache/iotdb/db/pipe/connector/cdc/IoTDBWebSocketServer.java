package org.apache.iotdb.db.pipe.connector.cdc;

import org.apache.iotdb.tsfile.write.record.Tablet;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class IoTDBWebSocketServer extends WebSocketServer {
  private volatile WebSocket client;
//  private BlockingQueue<Tablet> tablets = new LinkedBlockingQueue<Tablet>();

  public IoTDBWebSocketServer(InetSocketAddress address) {
    super(address);
  }

  @Override
  public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
    String log;
    if (client == null) {
      log =
          String.format(
              "Client from %s:%d connected!",
              webSocket.getRemoteSocketAddress().getHostName(),
              webSocket.getRemoteSocketAddress().getPort());
      client = webSocket;
    } else {
      log = "Too many connections!";
      webSocket.close(1014, log);
    }
    System.out.println(log);
  }

  @Override
  public void onClose(WebSocket webSocket, int i, String s, boolean b) {
    String log =
        String.format(
            "Client from %s:%d closed!",
            webSocket.getRemoteSocketAddress().getHostName(),
            webSocket.getRemoteSocketAddress().getPort());
    System.out.println(log);
    if (webSocket.equals(client)) {
      client = null;
    }
  }

  @Override
  public void onMessage(WebSocket webSocket, String s) {
//    if (client != webSocket && "ACK".equals(s)) {
//      try {
//        this.broadcast(tablets.take().serialize());
//      } catch (InterruptedException | IOException e) {
//        throw new RuntimeException(e);
//      }
//    }
  }

  @Override
  public void onError(WebSocket webSocket, Exception e) {
    System.out.println(e.getMessage());
    client = null;
  }

  @Override
  public void onStart() {
    String log = "IoTDBWebSocketServer started!";
    System.out.println(log);
  }

  public void addTablet(Tablet tablet) {
//    tablets.add(tablet);
  }

  public boolean hasClient() {
    return client != null;
  }
}
