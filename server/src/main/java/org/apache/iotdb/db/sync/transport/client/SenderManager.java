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
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.sender.pipe.IoTDBPipeSink;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SenderManager {
  private static final Logger logger = LoggerFactory.getLogger(SenderManager.class);

  protected ITransportClient transportClient;
  private Pipe pipe;
  private PipeSink pipeSink;

  protected ExecutorService transportExecutorService;
  private Future transportFuture;

  public SenderManager(Pipe pipe, IoTDBPipeSink pipeSink) {
    this.pipe = pipe;
    this.pipeSink = pipeSink;
    this.transportExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.SYNC_SENDER_PIPE.getName() + "-" + pipe.getName());
    this.transportClient = TransportClientFactory.createTransportClient(pipe, pipeSink);
  }

  public void start() {
    transportFuture = transportExecutorService.submit(this::takePipeDataAndTransport);
  }

  public void stop() {
    if (transportFuture != null) {
      transportFuture.cancel(true);
    }
  }

  public boolean close() throws InterruptedException {
    boolean isClosed;
    transportExecutorService.shutdownNow();
    isClosed =
        transportExecutorService.awaitTermination(
            SyncConstant.DEFAULT_WAITING_FOR_STOP_MILLISECONDS, TimeUnit.MILLISECONDS);
    return isClosed;
  }

  private void takePipeDataAndTransport() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          if (!transportClient.handshake()) {
            SyncService.getInstance()
                .receiveMsg(
                    PipeMessage.MsgType.ERROR,
                    String.format("Can not handshake with %s", pipeSink));
          }
          while (!Thread.currentThread().isInterrupted()) {
            PipeData pipeData = pipe.take();
            if (!transportClient.send(pipeData)) {
              logger.error(String.format("Can not transfer pipedata %s, skip it.", pipeData));
              // can do something.
              SyncService.getInstance()
                  .receiveMsg(
                      PipeMessage.MsgType.WARN,
                      String.format(
                          "Transfer piepdata %s error, skip it.", pipeData.getSerialNumber()));
              continue;
            }
            pipe.commit();
          }
        } catch (SyncConnectionException e) {
          logger.error(String.format("Connect to receiver %s error, because %s.", pipeSink, e));
          // TODO: wait and retry
        }
      }
    } catch (InterruptedException e) {
      logger.info("Interrupted by pipe, exit transport.");
    } finally {
      transportClient.close();
    }
  }

  /** test */
  @TestOnly
  public void setTransportClient(ITransportClient transportClient) {
    this.transportClient = transportClient;
  }
}
