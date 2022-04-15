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
package org.apache.iotdb.db.sync.sender.service;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.transport.client.ITransportClient;
import org.apache.iotdb.service.transport.thrift.RequestType;
import org.apache.iotdb.service.transport.thrift.SyncRequest;
import org.apache.iotdb.service.transport.thrift.SyncResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TransportHandler {
  private static final Logger logger = LoggerFactory.getLogger(TransportHandler.class);

  private final String pipeName;
  private final String localIp;
  private final long createTime;
  private final ITransportClient transportClient;

  private final ExecutorService transportExecutorService;
  private Future transportFuture;

  private final ScheduledExecutorService heartbeatExecutorService;
  private Future heartbeatFuture;

  public TransportHandler(ITransportClient transportClient, String pipeName, long createTime) {
    this.pipeName = pipeName;
    this.createTime = createTime;
    this.transportClient = transportClient;

    this.transportExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.SYNC_SENDER_PIPE.getName() + "-" + pipeName);
    this.heartbeatExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.SYNC_SENDER_HEARTBEAT.getName() + "-" + pipeName);

    String localIp1;
    try {
      localIp1 = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      logger.error(
          String.format("Get local host error when create transport handler, because %s.", e));
      localIp1 = SyncConstant.UNKNOWN_IP;
    }
    this.localIp = localIp1;
  }

  public void start() {
    transportFuture = transportExecutorService.submit(transportClient);
    heartbeatFuture =
        heartbeatExecutorService.scheduleWithFixedDelay(
            this::sendHeartbeat,
            SyncConstant.DEFAULT_HEARTBEAT_DELAY_SECONDS,
            SyncConstant.DEFAULT_HEARTBEAT_DELAY_SECONDS,
            TimeUnit.SECONDS);
  }

  public void stop() {
    if (transportFuture != null) {
      transportFuture.cancel(true);
    }
    if (heartbeatFuture != null) {
      heartbeatFuture.cancel(true);
    }
  }

  public boolean close() throws InterruptedException {
    boolean isClosed;
    transportExecutorService.shutdownNow();
    isClosed =
        transportExecutorService.awaitTermination(
            SyncConstant.DEFAULT_WAITING_FOR_STOP_MILLISECONDS, TimeUnit.MILLISECONDS);
    heartbeatExecutorService.shutdownNow();
    isClosed &=
        heartbeatExecutorService.awaitTermination(
            SyncConstant.DEFAULT_WAITING_FOR_STOP_MILLISECONDS, TimeUnit.MILLISECONDS);
    return isClosed;
  }

  public SyncResponse sendMsg(RequestType type) throws SyncConnectionException {
    return transportClient.heartbeat(new SyncRequest(type, pipeName, localIp, createTime));
  }

  private void sendHeartbeat() {
    try {
      SenderService.getInstance()
          .receiveMsg(
              transportClient.heartbeat(
                  new SyncRequest(RequestType.HEARTBEAT, pipeName, localIp, createTime)));
    } catch (SyncConnectionException e) {
      logger.warn(
          String.format(
              "Pipe %s sends heartbeat to receiver error, skip this time, because %s.",
              pipeName, e));
    }
  }
}
