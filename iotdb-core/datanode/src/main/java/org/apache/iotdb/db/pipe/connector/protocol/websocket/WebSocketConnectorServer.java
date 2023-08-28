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
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
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

public class WebSocketConnectorServer extends WebSocketServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnectorServer.class);
  private final PriorityBlockingQueue<Pair<Long, Event>> events =
      new PriorityBlockingQueue<>(11, Comparator.comparing(o -> o.left));
  private final WebsocketConnector websocketConnector;

  private final ConcurrentMap<Long, Event> eventMap = new ConcurrentHashMap<>();

  public WebSocketConnectorServer(
      InetSocketAddress address, WebsocketConnector websocketConnector) {
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
    String log =
        String.format(
            "Received a message `%s` from %s:%d",
            s,
            webSocket.getRemoteSocketAddress().getHostName(),
            webSocket.getRemoteSocketAddress().getPort());
    LOGGER.info(log);
    if (s.startsWith("START")) {
      handleStart(webSocket);
    } else if (s.startsWith("ACK")) {
      handleAck(webSocket, s);
    } else if (s.startsWith("ERROR")) {
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
    LOGGER.error(log);
  }

  public void addEvent(Pair<Long, Event> event) {
    if (events.size() >= 50) {
      synchronized (events) {
        while (events.size() >= 50) {
          try {
            events.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
    events.put(event);
  }

  private void handleStart(WebSocket webSocket) {
    try {
      Pair<Long, Event> eventPair = events.take();
      synchronized (events) {
        events.notifyAll();
        transfer(eventPair, webSocket);
      }
    } catch (InterruptedException e) {
      String log = String.format("The event can't be taken, because: %s", e.getMessage());
      LOGGER.warn(log);
      Thread.currentThread().interrupt();
    }
  }

  private void handleAck(WebSocket webSocket, String s) {
    long commitId = Long.parseLong(s.replace("ACK:", ""));
    Event event = eventMap.remove(commitId);
    websocketConnector.commit(
        commitId, event instanceof EnrichedEvent ? (EnrichedEvent) event : null);
    handleStart(webSocket);
  }

  private void handleError(WebSocket webSocket, String s) {
    long commitId = Long.parseLong(s.replace("ERROR:", ""));
    String log =
        String.format(
            "The tablet of commitId: %d can't be parsed by client, it will be retried later.",
            commitId);
    LOGGER.warn(log);
    events.put(new Pair<>(commitId, eventMap.remove(commitId)));
    handleStart(webSocket);
  }

  private void transfer(Pair<Long, Event> eventPair, WebSocket webSocket) {
    Long commitId = eventPair.getLeft();
    Event event = eventPair.getRight();
    try {
      ByteBuffer tabletBuffer = null;
      if (event instanceof PipeInsertNodeTabletInsertionEvent) {
        tabletBuffer = ((PipeInsertNodeTabletInsertionEvent) event).convertToTablet().serialize();
      } else if (event instanceof PipeRawTabletInsertionEvent) {
        tabletBuffer = ((PipeRawTabletInsertionEvent) event).convertToTablet().serialize();
      } else if (event instanceof PipeTsFileInsertionEvent) {
        PipeTsFileInsertionEvent tsFileInsertionEvent = (PipeTsFileInsertionEvent) event;
        tsFileInsertionEvent.waitForTsFileClose();
        Iterable<TabletInsertionEvent> subEvents = tsFileInsertionEvent.toTabletInsertionEvents();
        for (TabletInsertionEvent subEvent : subEvents) {
          tabletBuffer = ((PipeRawTabletInsertionEvent) subEvent).convertToTablet().serialize();
        }
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
      this.broadcast(payload, Collections.singletonList(webSocket));
      eventMap.put(eventPair.getLeft(), eventPair.getRight());
      String log =
          String.format(
              "Transferred a message to client %s:%d",
              webSocket.getRemoteSocketAddress().getAddress().getHostName(),
              webSocket.getRemoteSocketAddress().getPort());
      LOGGER.info(log);
    } catch (InterruptedException e) {
      events.put(eventPair);
      Thread.currentThread().interrupt();
      throw new PipeException(e.getMessage());
    } catch (Exception e) {
      events.put(eventPair);
      e.printStackTrace();
      throw new PipeException(e.getMessage());
    }
  }
}
