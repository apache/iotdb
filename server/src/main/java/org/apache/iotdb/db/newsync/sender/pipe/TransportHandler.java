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
package org.apache.iotdb.db.newsync.sender.pipe;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.transport.client.ITransportClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TransportHandler {
  private final ITransportClient transportClient;

  private final ExecutorService transportExecutorService;
  private Future transportFuture;

  private final ScheduledExecutorService heartbeatExecutorService;
  private Future heartbeatFuture;

  public TransportHandler(ITransportClient transportClient, String pipeName) {
    this.transportClient = transportClient;
    this.transportExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.SYNC_SENDER_PIPE.getName() + "-" + pipeName);
    this.heartbeatExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.SYNC_SENDER_HEARTBEAT.getName() + "-" + pipeName);
  }

  public void start() {
    transportFuture = transportExecutorService.submit(transportClient);
    heartbeatFuture =
        heartbeatExecutorService.scheduleWithFixedDelay(
            this::sendHeartbeat,
            0L,
            SyncConstant.DEFAULT_HEARTBEAT_DELAY_SECONDS,
            TimeUnit.SECONDS);
  }

  public void stop() {
    transportFuture.cancel(true);
    heartbeatFuture.cancel(true);
  }

  public boolean close() throws InterruptedException {
    boolean isClosed;
    transportExecutorService.shutdownNow();
    isClosed =
        transportExecutorService.awaitTermination(
            SyncConstant.DEFAULT_WAITTING_FOR_STOP_MILLISECONDS, TimeUnit.MILLISECONDS);
    heartbeatExecutorService.shutdownNow();
    isClosed &=
        heartbeatExecutorService.awaitTermination(
            SyncConstant.DEFAULT_WAITTING_FOR_STOP_MILLISECONDS, TimeUnit.MILLISECONDS);
    return isClosed;
  }

  private void sendHeartbeat() {
    //    transportClient.heartbeat(new SyncRequest(HEARTBEAT))
  }
}
