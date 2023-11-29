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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class WebSocketConnectorServer extends WebSocketServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnectorServer.class);

  private final AtomicLong eventIdGenerator = new AtomicLong(0);
  // Map<pipeName, Queue<Tuple<eventId, connector, event>>>
  private final ConcurrentHashMap<String, PriorityBlockingQueue<EventWaitingForTransfer>>
      eventsWaitingForTransfer = new ConcurrentHashMap<>();
  // Map<pipeName, Map<eventId, Tuple<connector, event>>>
  private final ConcurrentHashMap<String, ConcurrentHashMap<Long, EventWaitingForAck>>
      eventsWaitingForAck = new ConcurrentHashMap<>();

  private final BidiMap<String, WebSocket> router =
      new DualTreeBidiMap<String, WebSocket>(null, Comparator.comparing(Object::hashCode)) {};

  private static final AtomicReference<WebSocketConnectorServer> instance = new AtomicReference<>();
  private static final AtomicBoolean isStarted = new AtomicBoolean(false);

  private WebSocketConnectorServer(int port) {
    super(new InetSocketAddress(port));
    new TransferThread(this).start();
  }

  public static synchronized WebSocketConnectorServer getOrCreateInstance(int port) {
    if (null == instance.get()) {
      instance.set(new WebSocketConnectorServer(port));
    }
    return instance.get();
  }

  public void register(WebSocketConnector connector) {
    eventsWaitingForTransfer.putIfAbsent(
        connector.getPipeName(),
        new PriorityBlockingQueue<>(11, Comparator.comparing(o -> o.eventId)));
    eventsWaitingForAck.putIfAbsent(connector.getPipeName(), new ConcurrentHashMap<>());
  }

  public void unregister(WebSocketConnector connector) {
    eventsWaitingForTransfer.remove(connector.getPipeName());
    eventsWaitingForAck.remove(connector.getPipeName());
  }

  @Override
  public void start() {
    super.start();
    isStarted.set(true);
  }

  @Override
  public synchronized void onStart() {
    LOGGER.info(
        "The websocket server {}:{} has been started!", getAddress().getHostName(), getPort());
  }

  public boolean isStarted() {
    return isStarted.get();
  }

  @Override
  public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
    LOGGER.info(
        "The websocket connection from client {}:{} has been opened!",
        webSocket.getRemoteSocketAddress().getHostName(),
        webSocket.getRemoteSocketAddress().getPort());
  }

  @Override
  public void onClose(WebSocket webSocket, int i, String s, boolean b) {
    LOGGER.info(
        "The websocket connection from client {}:{} has been closed!",
        webSocket.getRemoteSocketAddress().getHostName(),
        webSocket.getRemoteSocketAddress().getPort());
    router.remove(router.getKey(webSocket));
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
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Received a ack message from {}:{}",
            webSocket.getRemoteSocketAddress().getHostName(),
            webSocket.getRemoteSocketAddress().getPort());
      }

      handleAck(webSocket, Long.parseLong(s.replace("ACK:", "")));
    } else if (s.startsWith("ERROR")) {
      LOGGER.warn(
          "Received an error message {} from {}:{}",
          s,
          webSocket.getRemoteSocketAddress().getHostName(),
          webSocket.getRemoteSocketAddress().getPort());

      handleError(webSocket, Long.parseLong(s.replace("ERROR:", "")));
    } else {
      LOGGER.warn(
          "Received an unknown message {} from {}:{}",
          s,
          webSocket.getRemoteSocketAddress().getHostName(),
          webSocket.getRemoteSocketAddress().getPort());
    }
  }

  private void handleBind(WebSocket webSocket, String pipeName) {
    if (router.containsKey(pipeName)) {
      broadcast("ERROR", Collections.singletonList(webSocket));
      webSocket.close(4000, "Too many connections.");
      return;
    }

    broadcast("READY", Collections.singletonList(webSocket));
    router.put(pipeName, webSocket);
  }

  private void handleAck(WebSocket webSocket, long eventId) {
    final String pipeName = router.getKey(webSocket);
    final EventWaitingForAck eventWrapper = eventsWaitingForAck.get(pipeName).remove(eventId);

    if (pipeName == null) {
      LOGGER.warn(
          "The websocket connection from {}:{} has been closed, "
              + "but the ack message of commitId: {} is received.",
          webSocket.getRemoteSocketAddress().getHostName(),
          webSocket.getRemoteSocketAddress().getPort(),
          eventId);
      return;
    }

    eventWrapper.connector.commit(
        eventWrapper.event instanceof EnrichedEvent ? (EnrichedEvent) eventWrapper.event : null);
  }

  private void handleError(WebSocket webSocket, long eventId) {
    LOGGER.warn(
        "The tablet of commitId: {} can't be parsed by client, it will be retried later.", eventId);

    final String pipeName = router.getKey(webSocket);
    final EventWaitingForAck eventWrapper = eventsWaitingForAck.get(pipeName).remove(eventId);

    if (pipeName == null) {
      LOGGER.warn(
          "The websocket connection from {}:{} has been closed, "
              + "but the error message of commitId: {} is received.",
          webSocket.getRemoteSocketAddress().getHostName(),
          webSocket.getRemoteSocketAddress().getPort(),
          eventId);
      return;
    }

    eventsWaitingForTransfer
        .get(pipeName)
        .put(new EventWaitingForTransfer(eventId, eventWrapper.connector, eventWrapper.event));
  }

  @Override
  public void onError(WebSocket webSocket, Exception e) {
    if (webSocket.getRemoteSocketAddress() != null) {
      LOGGER.warn(
          "Got an error \"{}\" from {}:{}.",
          e.getMessage(),
          webSocket.getLocalSocketAddress().getHostName(),
          webSocket.getLocalSocketAddress().getPort(),
          e);
    } else {
      LOGGER.warn("Got an error \"{}\" from an unknown client.", e.getMessage(), e);
      // if the remote socket address is null, it means the connection is not established yet.
      // we should close the connection manually.
      router.remove(router.getKey(webSocket));
    }
  }

  public void addEvent(Event event, WebSocketConnector connector) {
    final PriorityBlockingQueue<EventWaitingForTransfer> queue =
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

    queue.put(new EventWaitingForTransfer(eventIdGenerator.incrementAndGet(), connector, event));
  }

  private class TransferThread extends Thread {

    private final WebSocketConnectorServer server;

    public TransferThread(WebSocketConnectorServer server) {
      this.server = server;
    }

    @Override
    public void run() {
      while (true) {
        for (final String pipeName : eventsWaitingForTransfer.keySet()) {
          final PriorityBlockingQueue<EventWaitingForTransfer> queue =
              eventsWaitingForTransfer.getOrDefault(pipeName, null);
          if (queue == null || queue.isEmpty() || !router.containsKey(pipeName)) {
            continue;
          }

          try {
            final EventWaitingForTransfer queueElement = queue.take();
            synchronized (queue) {
              queue.notifyAll();
            }
            transfer(pipeName, queueElement);
          } catch (InterruptedException e) {
            LOGGER.warn("The transfer thread is interrupted.", e);
            Thread.currentThread().interrupt();

            throw new PipeException(e.getMessage());
          }
        }
      }
    }

    private void transfer(String pipeName, EventWaitingForTransfer element) {
      final Long eventId = element.eventId;
      final Event event = element.event;
      final WebSocketConnector connector = element.connector;

      try {
        ByteBuffer tabletBuffer;
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

        final ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + tabletBuffer.limit());
        payload.putLong(eventId);
        payload.put(tabletBuffer);
        payload.flip();

        server.broadcast(payload, Collections.singletonList(router.get(pipeName)));
        eventsWaitingForAck.get(pipeName).put(eventId, new EventWaitingForAck(connector, event));
      } catch (Exception e) {
        eventsWaitingForTransfer
            .get(pipeName)
            .put(new EventWaitingForTransfer(eventId, connector, event));
        throw new PipeException(e.getMessage(), e);
      }
    }
  }

  private static class EventWaitingForTransfer {

    private final Long eventId;
    private final WebSocketConnector connector;
    private final Event event;

    public EventWaitingForTransfer(Long eventId, WebSocketConnector connector, Event event) {
      this.eventId = eventId;
      this.connector = connector;
      this.event = event;
    }
  }

  private static class EventWaitingForAck {

    private final WebSocketConnector connector;
    private final Event event;

    public EventWaitingForAck(WebSocketConnector connector, Event event) {
      this.connector = connector;
      this.event = event;
    }
  }
}
