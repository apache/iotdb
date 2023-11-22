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
import org.apache.iotdb.tsfile.utils.Pair;

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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class WebSocketConnectorServer extends WebSocketServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnectorServer.class);
  private final AtomicLong eventIdGenerator = new AtomicLong(0);
  // Map<webSocketConnector, Queue<Pair<Pair<commitId, event>, retryTimes>>>
  private final ConcurrentMap<
          WebSocketConnector, PriorityBlockingQueue<Pair<Pair<Long, Event>, Integer>>>
      eventsWaitingForTransfer = new ConcurrentHashMap<>();
  // Map<webSocketConnector, Map<commitId, Pair<event, retryTimes>>>
  private final ConcurrentMap<WebSocketConnector, ConcurrentMap<Long, Pair<Event, Integer>>>
      eventsWaitingForAck = new ConcurrentHashMap<>();
  private final ConcurrentMap<WebSocketConnector, WebSocket> router = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, WebSocketConnector> pipeNameToConnector =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<WebSocket, String> webSocketToPipeName = new ConcurrentHashMap<>();

  private static WebSocketConnectorServer instance;

  private WebSocketConnectorServer(int port) {
    super(new InetSocketAddress(port));
    TransferThread transferThread = new TransferThread(this);
    transferThread.start();
  }

  public static WebSocketConnectorServer getInstance(int port) throws InterruptedException {
    if (null == instance) {
      synchronized (WebSocketConnectorServer.class) {
        if (null == instance) {
          instance = new WebSocketConnectorServer(port);
        }
      }
    }
    return instance;
  }

  public void register(WebSocketConnector connector) {
    eventsWaitingForTransfer.putIfAbsent(
            connector, new PriorityBlockingQueue<>(11, Comparator.comparing(o -> o.left.left)));
    eventsWaitingForAck.putIfAbsent(connector, new ConcurrentHashMap<>());
    pipeNameToConnector.put(connector.getPipeName(), connector);
  }

  public void unregister(WebSocketConnector connector) {
    eventsWaitingForTransfer.remove(connector);
    eventsWaitingForAck.remove(connector);
    pipeNameToConnector.remove(connector.getPipeName());
  }

  private class TransferThread extends Thread {
    WebSocketConnectorServer server;
    public TransferThread(WebSocketConnectorServer server) {
      this.server = server;
    }
    @Override
    public void run() {
      while (true) {
        for (WebSocketConnector connector : eventsWaitingForTransfer.keySet()) {
          PriorityBlockingQueue<Pair<Pair<Long, Event>, Integer>> queue =
                  eventsWaitingForTransfer.getOrDefault(connector, null);
          if (queue == null || queue.isEmpty()) {
            continue;
          }
          try {
            Pair<Pair<Long, Event>, Integer> pair = queue.take();
            synchronized (queue) {
              queue.notifyAll();
            }
            transfer(pair, router.get(connector));
            connector.commit(
                    pair.getLeft().getRight() instanceof EnrichedEvent
                            ? (EnrichedEvent) pair.getLeft().getRight()
                            : null);
          } catch (InterruptedException e) {
            String log =
                    String.format("The event can't be taken, because: %s", e.getMessage());
            LOGGER.warn(log);
            Thread.currentThread().interrupt();
            throw new PipeException(e.getMessage());
          }
        }
      }
    }
    private void transfer(Pair<Pair<Long, Event>, Integer> eventPair, WebSocket webSocket) {
      Long commitId = eventPair.getLeft().getLeft();
      Event event = eventPair.getLeft().getRight();
      Integer retryTimes = eventPair.getRight();
      String pipeName = webSocketToPipeName.get(webSocket);
      WebSocketConnector connector = pipeNameToConnector.get(pipeName);
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
          return;
        }

        ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + tabletBuffer.limit());
        payload.putLong(commitId);
        payload.put(tabletBuffer);
        payload.flip();
        server.broadcast(payload, Collections.singletonList(webSocket));
        eventsWaitingForAck.get(connector).put(commitId, new Pair<>(event, retryTimes - 1));
      } catch (Exception e) {
        eventsWaitingForTransfer
                .get(connector)
                .put(new Pair<>(new Pair<>(commitId, event), retryTimes - 1));
        throw new PipeException(e.getMessage());
      }
    }
  }

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
  public void onStart() {
    String log =
        String.format(
            "The webSocket server %s:%d has been started!",
            this.getAddress().getHostName(), this.getPort());
    LOGGER.info(log);
  }

  public void addEvent(Event event, WebSocketConnector connector) {
    if (eventsWaitingForTransfer.get(connector).size() >= 5) {
      synchronized (eventsWaitingForTransfer.get(connector)) {
        while (eventsWaitingForTransfer.get(connector).size() >= 5) {
          try {
            eventsWaitingForTransfer.get(connector).wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PipeException(e.getMessage());
          }
        }
      }
    }
    eventsWaitingForTransfer
        .get(connector)
        .put(new Pair<>(new Pair<>(eventIdGenerator.incrementAndGet(), event), 3));
    synchronized (eventsWaitingForTransfer.get(connector)) {
      eventsWaitingForTransfer.get(connector).notifyAll();
    }
  }

  private void handleBind(WebSocket webSocket, String pipeName) {
    if (router.containsKey(pipeNameToConnector.get(pipeName))) {
      webSocket.close(4000, "Too many connections.");
    }
    router.put(pipeNameToConnector.get(pipeName), webSocket);
    webSocketToPipeName.put(webSocket, pipeName);
  }

  private void handleAck(WebSocket webSocket, long commitId) {
    String pipeName = webSocketToPipeName.get(webSocket);
    WebSocketConnector connector = pipeNameToConnector.get(pipeName);
    Event event = eventsWaitingForAck.get(connector).remove(commitId).getLeft();
    if (event != null) {
      connector.commit(event instanceof EnrichedEvent ? (EnrichedEvent) event : null);
    }
  }

  private void handleError(WebSocket webSocket, long commitId) {
    String log =
        String.format(
            "The tablet of commitId: %d can't be parsed by client, it will be retried later.",
            commitId);
    LOGGER.warn(log);
    String pipeName = webSocketToPipeName.get(webSocket);
    WebSocketConnector connector = pipeNameToConnector.get(pipeName);
    Pair<Event, Integer> pair = eventsWaitingForAck.get(connector).remove(commitId);
    if (pair.getLeft() != null) {
      eventsWaitingForTransfer
          .get(connector)
          .put(new Pair<>(new Pair<>(commitId, pair.getLeft()), pair.getRight()));
    }
  }
}
