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
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.SyncConnectionException;
import org.apache.iotdb.commons.exception.sync.SyncHandshakeException;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
  private final Map<String, Future<?>> transportFutureMap;
  private ScheduledFuture<?> heartbeatFuture;
  private final Pipe pipe;
  private final PipeSink pipeSink;

  // store lock object for each waiting transport task thread
  private final BlockingQueue<Object> blockingQueue = new LinkedBlockingQueue<>();

  // Thread pool that send PipeData in parallel by DataRegion
  protected ExecutorService transportExecutorService;
  protected ScheduledExecutorService heartbeatExecutorService;

  protected long lastReportTime = 0;
  protected long lostConnectionTime = Long.MAX_VALUE;

  private boolean isRunning;

  private boolean isError = false;

  public SenderManager(Pipe pipe, PipeSink pipeSink) {
    this.pipe = pipe;
    this.pipeSink = pipeSink;
    this.transportExecutorService =
        IoTDBThreadPoolFactory.newCachedThreadPool(
            ThreadName.SYNC_SENDER_PIPE.getName() + "-" + pipe.getName());
    this.heartbeatExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.SYNC_SENDER_HEARTBEAT.getName() + "-" + pipe.getName());
    this.clientMap = new HashMap<>();
    this.transportFutureMap = new HashMap<>();
    this.isRunning = false;
  }

  public void checkConnection() {
    ISyncClient client = SyncClientFactory.createHeartbeatClient(pipe, pipeSink);
    try {
      client.handshake();
    } catch (SyncConnectionException syncConnectionException) {
      logger.warn(
          "Cannot connect to the receiver {} when starting PIPE check because {}, PIPE will keep RUNNING and try to reconnect",
          pipeSink,
          syncConnectionException.getMessage());
    } finally {
      client.close();
    }
  }

  public void start() {
    blockingQueue.clear();
    lastReportTime = System.currentTimeMillis();
    lostConnectionTime = Long.MAX_VALUE;
    for (Map.Entry<String, ISyncClient> entry : clientMap.entrySet()) {
      String dataRegionId = entry.getKey();
      ISyncClient syncClient = entry.getValue();
      transportFutureMap.put(
          dataRegionId,
          transportExecutorService.submit(
              () -> takePipeDataAndTransport(syncClient, dataRegionId)));
    }
    heartbeatFuture =
        ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
            heartbeatExecutorService,
            this::heartbeat,
            0,
            SyncConstant.HEARTBEAT_INTERVAL_MILLISECONDS,
            TimeUnit.MILLISECONDS);
    isRunning = true;
  }

  public void stop() {
    if (heartbeatFuture != null) {
      heartbeatFuture.cancel(true);
    }
    for (Future<?> future : transportFutureMap.values()) {
      future.cancel(true);
    }
    blockingQueue.clear();
    isRunning = false;
  }

  public void close() throws PipeException {
    try {
      boolean isClosed;
      transportExecutorService.shutdownNow();
      isClosed =
          transportExecutorService.awaitTermination(
              SyncConstant.DEFAULT_WAITING_FOR_STOP_MILLISECONDS, TimeUnit.MILLISECONDS);
      heartbeatExecutorService.shutdownNow();
      isClosed &=
          heartbeatExecutorService.awaitTermination(
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

  /**
   * heartbeat will be executed with delay time {@link SyncConstant#HEARTBEAT_INTERVAL_MILLISECONDS}
   * only if there are transport threads being blocked. It will notify all blocked transport thread
   * if successfully reconnect to receiver.
   *
   * <p>It will print warn log per {@link SyncConstant#LOST_CONNECT_REPORT_MILLISECONDS} ms.
   */
  private void heartbeat() {
    try {
      Object object = blockingQueue.take();
      ISyncClient client = SyncClientFactory.createHeartbeatClient(pipe, pipeSink);
      try {
        client.handshake();
        lostConnectionTime = Long.MAX_VALUE;
        logger.info("Reconnect to {} successfully.", pipeSink);
        synchronized (object) {
          object.notify();
        }
        while (!blockingQueue.isEmpty()) {
          object = blockingQueue.take();
          synchronized (object) {
            object.notify();
          }
        }
        isError = false;
      } catch (SyncConnectionException e) {
        if (e instanceof SyncHandshakeException && !isError) {
          SyncService.getInstance()
              .recordMessage(
                  pipe.getName(),
                  new PipeMessage(
                      PipeMessage.PipeMessageType.ERROR,
                      String.format("Can not handshake with %s", pipeSink)));
          isError = true;
        }
        blockingQueue.offer(object);
        long reportInterval = System.currentTimeMillis() - lastReportTime;
        if (reportInterval > SyncConstant.LOST_CONNECT_REPORT_MILLISECONDS) {
          logger.warn(
              "Connection error because {}. Lost contact with the receiver {} for {} milliseconds.",
              e.getMessage(),
              pipeSink,
              System.currentTimeMillis() - lostConnectionTime);
          lastReportTime = System.currentTimeMillis();
        }
      } finally {
        client.close();
      }
    } catch (InterruptedException e) {
      logger.info("Interrupted by PIPE operation, exit heartbeat.");
    }
  }

  private void takePipeDataAndTransport(ISyncClient syncClient, String dataRegionId) {
    try {
      Object lock = new Object();
      synchronized (lock) {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            syncClient.handshake();
            while (!Thread.currentThread().isInterrupted()) {
              PipeData pipeData = pipe.take(dataRegionId);
              if (!syncClient.send(pipeData)) {
                logger.error("Can not transfer PipeData {}, skip it.", pipeData);
                // can do something.
                SyncService.getInstance()
                    .recordMessage(
                        pipe.getName(),
                        new PipeMessage(
                            PipeMessage.PipeMessageType.WARN,
                            String.format(
                                "Transfer PipeData %s error, skip it.",
                                pipeData.getSerialNumber())));
              }
              pipe.commit(dataRegionId);
            }
          } catch (SyncConnectionException e) {
            // If failed to connect to receiver or failed to handshake with receiver, it will hang
            // up until scheduled heartbeat task
            // successfully reconnect to receiver.
            logger.error("Connect to receiver {} error, because {}.", pipeSink, e.getMessage());
            lostConnectionTime = Math.min(lostConnectionTime, System.currentTimeMillis());
            blockingQueue.offer(lock);
            lock.wait();
          }
        }
      }
    } catch (InterruptedException e) {
      logger.info("Interrupted by PIPE operation, exit transport.");
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
    Future<?> future = transportFutureMap.remove(dataRegionId);
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
