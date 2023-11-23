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
  private final PriorityBlockingQueue<Pair<Long, Event>> eventsWaitingForTransfer =
      new PriorityBlockingQueue<>(11, Comparator.comparing(o -> o.left));
  private final ConcurrentMap<Long, Event> eventsWaitingForAck = new ConcurrentHashMap<>();

  private final WebSocketConnector websocketConnector;

  public WebSocketConnectorServer(
      InetSocketAddress address, WebSocketConnector websocketConnector) {
    super(address);
    this.websocketConnector = websocketConnector;
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
    if (s.startsWith("START")) {
      LOGGER.info(
          "Received a start message from {}:{}",
          webSocket.getRemoteSocketAddress().getHostName(),
          webSocket.getRemoteSocketAddress().getPort());
      handleStart(webSocket);
    } else if (s.startsWith("ACK")) {
      handleAck(webSocket, s);
    } else if (s.startsWith("ERROR")) {
      LOGGER.error(
          "Received an error message {} from {}:{}",
          s,
          webSocket.getRemoteSocketAddress().getHostName(),
          webSocket.getRemoteSocketAddress().getPort());
      handleError(webSocket, s);
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

  public void addEvent(Event event) {
    if (eventsWaitingForTransfer.size() >= 5) {
      synchronized (eventsWaitingForTransfer) {
        while (eventsWaitingForTransfer.size() >= 5) {
          try {
            eventsWaitingForTransfer.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PipeException(e.getMessage());
          }
        }
      }
    }

    eventsWaitingForTransfer.put(new Pair<>(eventIdGenerator.incrementAndGet(), event));
  }

  private void handleStart(WebSocket webSocket) {
    try {
      while (true) {
        Pair<Long, Event> eventPair = eventsWaitingForTransfer.take();
        synchronized (eventsWaitingForTransfer) {
          eventsWaitingForTransfer.notifyAll();
        }
        boolean transferred = transfer(eventPair, webSocket);
        if (transferred) {
          break;
        } else {
          websocketConnector.commit(
              eventPair.getRight() instanceof EnrichedEvent
                  ? (EnrichedEvent) eventPair.getRight()
                  : null);
        }
      }
    } catch (InterruptedException e) {
      String log = String.format("The event can't be taken, because: %s", e.getMessage());
      LOGGER.warn(log);
      Thread.currentThread().interrupt();
      throw new PipeException(e.getMessage());
    }
  }

  private boolean transfer(Pair<Long, Event> eventPair, WebSocket webSocket) {
    Long commitId = eventPair.getLeft();
    Event event = eventPair.getRight();
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
        return false;
      }

      ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + tabletBuffer.limit());
      payload.putLong(commitId);
      payload.put(tabletBuffer);
      payload.flip();

      this.broadcast(payload, Collections.singletonList(webSocket));
      eventsWaitingForAck.put(eventPair.getLeft(), eventPair.getRight());
    } catch (Exception e) {
      eventsWaitingForTransfer.put(eventPair);
      throw new PipeException(e.getMessage());
    }
    return true;
  }

  private void handleAck(WebSocket webSocket, String s) {
    long commitId = Long.parseLong(s.replace("ACK:", ""));
    Event event = eventsWaitingForAck.remove(commitId);
    if (event != null) {
      websocketConnector.commit(event instanceof EnrichedEvent ? (EnrichedEvent) event : null);
    }
    handleStart(webSocket);
  }

  private void handleError(WebSocket webSocket, String s) {
    long commitId = Long.parseLong(s.replace("ERROR:", ""));
    String log =
        String.format(
            "The tablet of commitId: %d can't be parsed by client, it will be retried later.",
            commitId);
    LOGGER.warn(log);
    Event event = eventsWaitingForAck.remove(commitId);
    if (event != null) {
      eventsWaitingForTransfer.put(new Pair<>(commitId, event));
    }
    handleStart(webSocket);
  }
}
