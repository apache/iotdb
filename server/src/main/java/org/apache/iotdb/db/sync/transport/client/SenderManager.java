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
 *
 */
package org.apache.iotdb.db.sync.transport.client;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * SenderManager handles rpc send logic in sender-side. Each SenderManager and Pipe have a
 * one-to-one relationship. It takes PipeData from Pipe and sends PipeData to receiver.
 */
public class SenderManager {
  private static final Logger logger = LoggerFactory.getLogger(SenderManager.class);

  // <DataRegionId, ISyncClient>
  private final Map<String, ISyncClient> clientMap;
  // <DataRegionId, ISyncClient>
  private final Map<String, Future> transportFutureMap;
  private final Pipe pipe;
  private final PipeSink pipeSink;

  // Thread pool that send PipeData in parallel by DataRegion
  protected ExecutorService transportExecutorService;

  private boolean isRunning;

  public SenderManager(Pipe pipe, PipeSink pipeSink) {
    this.pipe = pipe;
    this.pipeSink = pipeSink;
    this.transportExecutorService =
        IoTDBThreadPoolFactory.newCachedThreadPool(
            ThreadName.SYNC_SENDER_PIPE.getName() + "-" + pipe.getName());
    this.clientMap = new HashMap<>();
    this.transportFutureMap = new HashMap<>();
    this.isRunning = false;
  }

  public void start() {
    for (Map.Entry<String, ISyncClient> entry : clientMap.entrySet()) {
      String dataRegionId = entry.getKey();
      ISyncClient syncClient = entry.getValue();
      transportFutureMap.put(
          dataRegionId,
          transportExecutorService.submit(
              () -> takePipeDataAndTransport(syncClient, dataRegionId)));
    }
    isRunning = true;
  }

  public void stop() {
    for (Future future : transportFutureMap.values()) {
      future.cancel(true);
    }
    isRunning = false;
  }

  public void close() throws PipeException {
    try {
      boolean isClosed;
      transportExecutorService.shutdownNow();
      isClosed =
          transportExecutorService.awaitTermination(
              SyncConstant.DEFAULT_WAITING_FOR_STOP_MILLISECONDS, TimeUnit.MILLISECONDS);
      if (!isClosed) {
        throw new PipeException(
            String.format(
                "Close SenderManager of Pipe %s error after %s %s, please try again.",
                pipe.getName(),
                SyncConstant.DEFAULT_WAITING_FOR_STOP_MILLISECONDS,
                TimeUnit.MILLISECONDS.name()));
      }
    } catch (InterruptedException e) {
      throw new PipeException(
          String.format(
              "Interrupted when waiting for clear SenderManager of Pipe %s.", pipe.getName()));
    }
  }

  private void takePipeDataAndTransport(ISyncClient syncClient, String dataRegionId) {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          if (!syncClient.handshake()) {
            SyncService.getInstance()
                .receiveMsg(
                    PipeMessage.MsgType.ERROR,
                    String.format("Can not handshake with %s", pipeSink));
          }
          while (!Thread.currentThread().isInterrupted()) {
            PipeData pipeData = pipe.take(dataRegionId);
            if (!syncClient.send(pipeData)) {
              logger.error(String.format("Can not transfer pipedata %s, skip it.", pipeData));
              // can do something.
              SyncService.getInstance()
                  .receiveMsg(
                      PipeMessage.MsgType.WARN,
                      String.format(
                          "Transfer piepdata %s error, skip it.", pipeData.getSerialNumber()));
            }
            pipe.commit(dataRegionId);
          }
        } catch (SyncConnectionException e) {
          logger.error(String.format("Connect to receiver %s error, because %s.", pipeSink, e));
          // TODO: wait and retry
        }
      }
    } catch (InterruptedException e) {
      logger.info("Interrupted by pipe, exit transport.");
    } finally {
      syncClient.close();
    }
  }

  public void registerDataRegion(String dataRegionId) {
    ISyncClient syncClient = SyncClientFactory.createSyncClient(pipe, pipeSink, dataRegionId);
    clientMap.put(dataRegionId, syncClient);
    if (isRunning) {
      transportFutureMap.put(
          dataRegionId,
          transportExecutorService.submit(
              () -> takePipeDataAndTransport(syncClient, dataRegionId)));
    }
  }

  public void unregisterDataRegion(String dataRegionId) {
    Future future = transportFutureMap.remove(dataRegionId);
    if (future != null) {
      future.cancel(true);
      clientMap.remove(dataRegionId);
    }
  }

  /** test */
  @TestOnly
  public void setSyncClient(ISyncClient syncClient) {
    // TODO: update test env
  }
}
