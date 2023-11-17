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

import org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WebSocketConnector implements PipeConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnector.class);

  private static final Map<Integer, Pair<AtomicInteger, WebSocketConnectorServer>>
      PORT_TO_REFERENCE_COUNT_AND_SERVER_MAP = new ConcurrentHashMap<>();

  private Integer port;
  private WebSocketConnectorServer server;

  public final AtomicLong commitIdGenerator = new AtomicLong(0);
  private final AtomicLong lastCommitId = new AtomicLong(0);
  private final PriorityQueue<Pair<Long, Runnable>> commitQueue =
      new PriorityQueue<>(Comparator.comparing(o -> o.left));

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Do nothing
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    port =
        parameters.getIntOrDefault(
            Arrays.asList(
                PipeConnectorConstant.CONNECTOR_WEBSOCKET_PORT_KEY,
                PipeConnectorConstant.SINK_WEBSOCKET_PORT_KEY),
            PipeConnectorConstant.CONNECTOR_WEBSOCKET_PORT_DEFAULT_VALUE);
  }

  @Override
  public void handshake() throws Exception {
    synchronized (PORT_TO_REFERENCE_COUNT_AND_SERVER_MAP) {
      server =
          PORT_TO_REFERENCE_COUNT_AND_SERVER_MAP
              .computeIfAbsent(
                  port,
                  key -> {
                    final WebSocketConnectorServer newServer =
                        new WebSocketConnectorServer(new InetSocketAddress(port), this);
                    newServer.start();
                    return new Pair<>(new AtomicInteger(0), newServer);
                  })
              .getRight();
      PORT_TO_REFERENCE_COUNT_AND_SERVER_MAP.get(port).getLeft().incrementAndGet();
    }
  }

  @Override
  public void heartbeat() throws Exception {}

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "WebsocketConnector only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Current event: {}.",
          tabletInsertionEvent);
      return;
    }
    long commitId = commitIdGenerator.incrementAndGet();
    ((EnrichedEvent) tabletInsertionEvent)
        .increaseReferenceCount(WebSocketConnector.class.getName());
    server.addEvent(new Pair<>(commitId, tabletInsertionEvent));
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "WebsocketConnector only support PipeTsFileInsertionEvent. Current event: {}.",
          tsFileInsertionEvent);
      return;
    }
    try {
      for (TabletInsertionEvent event : tsFileInsertionEvent.toTabletInsertionEvents()) {
        long commitId = commitIdGenerator.incrementAndGet();
        ((EnrichedEvent) event).increaseReferenceCount(WebSocketConnector.class.getName());
        server.addEvent(new Pair<>(commitId, event));
      }
    } finally {
      tsFileInsertionEvent.close();
    }
  }

  @Override
  public void transfer(Event event) throws Exception {}

  @Override
  public void close() throws Exception {
    if (port == null) {
      return;
    }

    synchronized (PORT_TO_REFERENCE_COUNT_AND_SERVER_MAP) {
      final Pair<AtomicInteger, WebSocketConnectorServer> pair =
          PORT_TO_REFERENCE_COUNT_AND_SERVER_MAP.get(port);
      if (pair == null) {
        return;
      }

      if (pair.getLeft().decrementAndGet() <= 0) {
        try {
          pair.getRight().stop();
        } finally {
          PORT_TO_REFERENCE_COUNT_AND_SERVER_MAP.remove(port);
        }
      }
    }
  }

  public synchronized void commit(long requestCommitId, @Nullable EnrichedEvent enrichedEvent) {
    commitQueue.offer(
        new Pair<>(
            requestCommitId,
            () ->
                Optional.ofNullable(enrichedEvent)
                    .ifPresent(
                        event ->
                            event.decreaseReferenceCount(
                                WebSocketConnector.class.getName(), true))));

    while (!commitQueue.isEmpty()) {
      final Pair<Long, Runnable> committer = commitQueue.peek();

      // If the commit id is less than or equals to the last commit id, it means that
      // the event has been committed before, and has been retried. So the event can
      // be ignored.
      if (committer.left <= lastCommitId.get()) {
        commitQueue.poll();
        continue;
      }

      if (lastCommitId.get() + 1 != committer.left) {
        break;
      }

      committer.right.run();
      lastCommitId.incrementAndGet();

      commitQueue.poll();
    }
  }
}
