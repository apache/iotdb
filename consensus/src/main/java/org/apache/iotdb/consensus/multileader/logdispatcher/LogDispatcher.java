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

package org.apache.iotdb.consensus.multileader.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.multileader.IndexController;
import org.apache.iotdb.consensus.multileader.MultiLeaderServerImpl;
import org.apache.iotdb.consensus.multileader.Utils;
import org.apache.iotdb.consensus.multileader.client.AsyncMultiLeaderServiceClient;
import org.apache.iotdb.consensus.multileader.client.MultiLeaderConsensusClientPool.AsyncMultiLeaderServiceClientPoolFactory;
import org.apache.iotdb.consensus.multileader.conf.MultiLeaderConsensusConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LogDispatcher {

  private final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);

  private final MultiLeaderServerImpl impl;
  private final List<LogDispatcherThread> threads;
  private final ExecutorService executorService;
  private final IClientManager<TEndPoint, AsyncMultiLeaderServiceClient> clientManager;

  public LogDispatcher(MultiLeaderServerImpl impl) {
    this.impl = impl;
    this.threads =
        impl.getConfiguration().stream()
            .filter(x -> !Objects.equals(x, impl.getThisNode()))
            .map(LogDispatcherThread::new)
            .collect(Collectors.toList());
    this.executorService =
        IoTDBThreadPoolFactory.newFixedThreadPool(threads.size(), "LogDispatcher");
    this.clientManager =
        new IClientManager.Factory<TEndPoint, AsyncMultiLeaderServiceClient>()
            .createClientManager(new AsyncMultiLeaderServiceClientPoolFactory());
  }

  public void start() {
    threads.forEach(executorService::submit);
  }

  public void stop() {
    executorService.shutdownNow();
    clientManager.close();
    int timeout = 10;
    try {
      if (!executorService.awaitTermination(timeout, TimeUnit.SECONDS)) {
        logger.error("Unable to shutdown LogDispatcher service after {} seconds", timeout);
      }
    } catch (InterruptedException e) {
      logger.error("Unexpected shutdown when closing LogDispatcher service ");
    }
  }

  public OptionalLong getMinSyncIndex() {
    return threads.stream().mapToLong(LogDispatcherThread::getCurrentSyncIndex).min();
  }

  public void offer(IndexedConsensusRequest request) {
    threads.forEach(
        thread -> {
          if (!thread.offer(request)) {
            logger.debug("Log queue of {} is full, ignore the log to this node", thread.getPeer());
          }
        });
  }

  class LogDispatcherThread implements Runnable {

    private final Peer peer;
    private final IndexController controller;
    private final BlockingQueue<IndexedConsensusRequest> pendingRequest =
        new ArrayBlockingQueue<>(MultiLeaderConsensusConfig.MAX_PENDING_REQUEST_NUM_PER_NODE);

    public LogDispatcherThread(Peer peer) {
      this.peer = peer;
      this.controller =
          new IndexController(
              impl.getStorageDir(), Utils.fromTEndPointToString(peer.getEndpoint()), true);
    }

    public long getCurrentSyncIndex() {
      return controller.getCurrentIndex();
    }

    public Peer getPeer() {
      return peer;
    }

    public boolean offer(IndexedConsensusRequest request) {
      return pendingRequest.offer(request);
    }

    @Override
    public void run() {
      try {
        while (!Thread.interrupted()) {
          IndexedConsensusRequest poll = pendingRequest.take();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Unexpected error in log dispatcher", e);
      }
    }
  }
}
