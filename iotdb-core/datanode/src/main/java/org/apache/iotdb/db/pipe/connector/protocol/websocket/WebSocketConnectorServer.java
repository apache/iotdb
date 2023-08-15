package org.apache.iotdb.db.pipe.connector.protocol.websocket;

import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class WebSocketConnectorServer extends WebSocketServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnectorServer.class);
  private final PriorityBlockingQueue<Pair<Long, Event>> events =
          new PriorityBlockingQueue<>(11, Comparator.comparing(o -> o.left));
  private WebsocketConnector websocketConnector;

  private ConcurrentMap<Long, Event> eventMap = new ConcurrentHashMap<>();

  public WebSocketConnectorServer(
      InetSocketAddress address, WebsocketConnector websocketConnector) {
    super(address);
    this.websocketConnector = websocketConnector;
  }

  @Override
  public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
    LOGGER.info("The websocket server has been started!");
  }

  @Override
  public void onClose(WebSocket webSocket, int i, String s, boolean b) {
    String log =
        String.format(
            "The client from %s:%d has been closed!",
            webSocket.getRemoteSocketAddress().getAddress(),
            webSocket.getRemoteSocketAddress().getPort());
    LOGGER.info(log);
  }

  @Override
  public void onMessage(WebSocket webSocket, String s) {
    if (!s.startsWith("ACK")) {
      return;
    }
    if (s.startsWith("ACK:")) {
      // TODO
      long completedCommitId = Long.valueOf(s.split(":")[1]);
      Event event = eventMap.remove(completedCommitId);
      websocketConnector.commit(completedCommitId, event instanceof EnrichedEvent ? (EnrichedEvent) event : null);
    }
    try {
      ArrayList<WebSocket> webSockets = new ArrayList<>();
      webSockets.add(webSocket);
      Pair<Long, Event> eventPair = events.take();
      transfer(eventPair, webSockets);
    } catch (InterruptedException e) {
      // TODO
      LOGGER.warn("");
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void onError(WebSocket webSocket, Exception e) {
    String log =
        String.format(
            "Got an error %s from %s:%d",
            e.getMessage(),
            webSocket.getRemoteSocketAddress().getAddress(),
            webSocket.getRemoteSocketAddress().getPort());
    LOGGER.error(log);
  }

  @Override
  public void onStart() {}

  public void addEvent(Pair<Long, Event> event) {
    events.put(event);
  }

  private void transfer(Pair<Long, Event> eventPair, List<WebSocket> webSockets) {
    Long commitId = eventPair.getLeft();
    Event event = eventPair.getRight();
    try {
      ByteBuffer tabletBuffer = null;
      if (event instanceof PipeInsertNodeTabletInsertionEvent) {
        tabletBuffer =
                ((PipeInsertNodeTabletInsertionEvent) event).convertToTablet().serialize();
      } else if (event instanceof PipeRawTabletInsertionEvent) {
        tabletBuffer =
                ((PipeRawTabletInsertionEvent) event).convertToTablet().serialize();
      } else if (event instanceof TsFileInsertionEvent) {
        Iterable<TabletInsertionEvent> subEvents =
                ((TsFileInsertionEvent) event).toTabletInsertionEvents();
        for (TabletInsertionEvent subEvent : subEvents) {
          tabletBuffer =
                  ((PipeRawTabletInsertionEvent) subEvent).convertToTablet().serialize();
        }
      } else {
        throw new NotImplementedException(
                "IoTDBCDCConnector only support "
                        + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent.");
      }
      ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + tabletBuffer.capacity());
      ReadWriteIOUtils.write(commitId, payload);
      ReadWriteIOUtils.write(tabletBuffer, payload);
      this.broadcast(payload, webSockets);
      events.put(eventPair);
    } catch (IOException e) {
      // TODO retry
      throw new PipeException(e.getMessage());
    }
  }
}
