/*
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

package org.apache.iotdb.db.pipe.connector.protocol.websocket;

import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualTreeBidiMap;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class WebSocketConnectorServer extends WebSocketServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnectorServer.class);
  private final AtomicLong eventIdGenerator = new AtomicLong(0);
  // Map<pipeName, Queue<Tuple<eventId, connector, event>>>
  private final ConcurrentHashMap<String, PriorityBlockingQueue<EventQueueElement>>
      eventsWaitingForTransfer = new ConcurrentHashMap<>();
  // Map<pipeName, Map<eventId, Tuple<connector, event>>>
  private final ConcurrentHashMap<String, ConcurrentHashMap<Long, EventMapElement>>
      eventsWaitingForAck = new ConcurrentHashMap<>();
  private final BidiMap<String, WebSocket> router =
      new DualTreeBidiMap<String, WebSocket>(null, Comparator.comparing(Object::hashCode)) {};

  private static WebSocketConnectorServer instance;
  public static Boolean isStarted = false;

  private WebSocketConnectorServer(int port) {
    super(new InetSocketAddress(port));
    TransferThread transferThread = new TransferThread(this);
    transferThread.start();
  }

  public static synchronized WebSocketConnectorServer getInstance(int port) {
    if (null == instance) {
      instance = new WebSocketConnectorServer(port);
    }
    return instance;
  }

  public void register(WebSocketConnector connector) {
    eventsWaitingForTransfer.putIfAbsent(
        connector.getPipeName(),
        new PriorityBlockingQueue<>(11, Comparator.comparing(o -> o.eventId)));
    eventsWaitingForAck.putIfAbsent(connector.getPipeName(), new ConcurrentHashMap<>());
  }

  public void unregister(WebSocketConnector connector) {}

  @Override
  public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
    String log =
        String.format(
            "The connection from client %s:%d has been opened!",
            webSocket.getRemoteSocketAddress().getHostName(),
            webSocket.getRemoteSocketAddress().getPort());
    LOGGER.info(log);
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
    if (s.startsWith("BIND")) {
      LOGGER.info(
          "Received a bind message from {}:{}",
          webSocket.getRemoteSocketAddress().getHostName(),
          webSocket.getRemoteSocketAddress().getPort());
      handleBind(webSocket, s.replace("BIND:", ""));
    } else if (s.startsWith("ACK")) {
      LOGGER.info(
          "Received a ack message from {}:{}",
          webSocket.getRemoteSocketAddress().getHostName(),
          webSocket.getRemoteSocketAddress().getPort());
      long commitId = Long.parseLong(s.replace("ACK:", ""));
      handleAck(webSocket, commitId);
    } else if (s.startsWith("ERROR")) {
      LOGGER.error(
          "Received an error message {} from {}:{}",
          s,
          webSocket.getRemoteSocketAddress().getHostName(),
          webSocket.getRemoteSocketAddress().getPort());
      long commitId = Long.parseLong(s.replace("ERROR:", ""));
      handleError(webSocket, commitId);
    }
  }

  @Override
  public void onError(WebSocket webSocket, Exception e) {
    String log;
    if (webSocket.getRemoteSocketAddress() != null) {
      log =
          String.format(
              "Got an error `%s` from %s:%d",
              e.getMessage(),
              webSocket.getLocalSocketAddress().getHostName(),
              webSocket.getLocalSocketAddress().getPort());
    } else {
      log = String.format("Got an error `%s` from client", e.getMessage());
    }
    LOGGER.error(log);
  }

  @Override
  public synchronized void onStart() {
    String log =
        String.format(
            "The webSocket server %s:%d has been started!",
            this.getAddress().getHostName(), this.getPort());
    LOGGER.info(log);
  }

  public void addEvent(Event event, WebSocketConnector connector) {
    PriorityBlockingQueue<EventQueueElement> queue =
        eventsWaitingForTransfer.get(connector.getPipeName());
    if (queue.size() >= 5) {
      synchronized (queue) {
        while (queue.size() >= 5) {
          try {
            queue.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PipeException(e.getMessage());
          }
        }
      }
    }
    queue.put(new EventQueueElement(eventIdGenerator.incrementAndGet(), connector, event));
  }

  private void handleBind(WebSocket webSocket, String pipeName) {
    if (router.containsKey(pipeName)) {
      webSocket.close(4000, "Too many connections.");
    }
    router.put(pipeName, webSocket);
  }

  private void handleAck(WebSocket webSocket, long eventId) {
    String pipeName = router.getKey(webSocket);
    EventMapElement mapElement = eventsWaitingForAck.get(pipeName).remove(eventId);
    mapElement.connector.commit(
        mapElement.event instanceof EnrichedEvent ? (EnrichedEvent) mapElement.event : null);
  }

  private void handleError(WebSocket webSocket, long eventId) {
    String log =
        String.format(
            "The tablet of commitId: %d can't be parsed by client, it will be retried later.",
            eventId);
    LOGGER.warn(log);
    String pipeName = router.getKey(webSocket);
    EventMapElement mapElement = eventsWaitingForAck.get(pipeName).remove(eventId);
    eventsWaitingForTransfer
        .get(pipeName)
        .put(new EventQueueElement(eventId, mapElement.connector, mapElement.event));
  }

  private class TransferThread extends Thread {
    WebSocketConnectorServer server;

    public TransferThread(WebSocketConnectorServer server) {
      this.server = server;
    }

    @Override
    public void run() {
      while (true) {
        for (String pipeName : eventsWaitingForTransfer.keySet()) {
          PriorityBlockingQueue<EventQueueElement> queue =
              eventsWaitingForTransfer.getOrDefault(pipeName, null);
          if (queue == null || queue.isEmpty() || !router.containsKey(pipeName)) {
            continue;
          }
          try {
            EventQueueElement queueElement = queue.take();
            synchronized (queue) {
              queue.notifyAll();
            }
            transfer(pipeName, queueElement);
          } catch (InterruptedException e) {
            String log = String.format("The event can't be taken, because: %s", e.getMessage());
            LOGGER.warn(log);
            Thread.currentThread().interrupt();
            throw new PipeException(e.getMessage());
          }
        }
      }
    }

    private void transfer(String pipeName, EventQueueElement element) {
      Long eventId = element.eventId;
      Event event = element.event;
      WebSocketConnector connector = element.connector;
      try {
        ByteBuffer tabletBuffer = null;
        if (event instanceof PipeInsertNodeTabletInsertionEvent) {
          tabletBuffer = ((PipeInsertNodeTabletInsertionEvent) event).convertToTablet().serialize();
        } else if (event instanceof PipeRawTabletInsertionEvent) {
          tabletBuffer = ((PipeRawTabletInsertionEvent) event).convertToTablet().serialize();
        } else {
          throw new NotImplementedException(
              "IoTDBCDCConnector only support "
                  + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent.");
        }
        if (tabletBuffer == null) {
          connector.commit((EnrichedEvent) event);
          return;
        }
        ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + tabletBuffer.limit());
        payload.putLong(eventId);
        payload.put(tabletBuffer);
        payload.flip();
        server.broadcast(payload, Collections.singletonList(router.get(pipeName)));
        eventsWaitingForAck.get(pipeName).put(eventId, new EventMapElement(connector, event));
      } catch (Exception e) {
        eventsWaitingForTransfer
            .get(pipeName)
            .put(new EventQueueElement(eventId, connector, event));
        throw new PipeException(e.getMessage());
      }
    }
  }

  private class EventQueueElement {
    Long eventId;
    WebSocketConnector connector;
    Event event;

    public EventQueueElement(Long eventId, WebSocketConnector connector, Event event) {
      this.eventId = eventId;
      this.connector = connector;
      this.event = event;
    }
  }

  private class EventMapElement {
    WebSocketConnector connector;
    Event event;

    public EventMapElement(WebSocketConnector connector, Event event) {
      this.connector = connector;
      this.event = event;
    }
  }
}
