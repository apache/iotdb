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
package org.apache.iotdb.db.integration.sync;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.sync.sender.pipe.IoTDBPipeSink;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.service.TransportHandler;

public class TransportHandlerMock extends TransportHandler {
  private Pipe pipe;

  public TransportHandlerMock(
      Pipe pipe, IoTDBPipeSink pipeSink, TransportClientMock transportClientMock) {
    super(pipe, pipeSink);
    this.transportClient = transportClientMock;
  }

  @Override
  protected void resetTransportClient(Pipe pipe) {
    try {
      super.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    super.resetTransportClient(pipe);
    IoTDBPipeSink pipeSink = (IoTDBPipeSink) pipe.getPipeSink();
    ((TransportClientMock) this.transportClient)
        .resetInfo(pipe, pipeSink.getIp(), pipeSink.getPort());
    this.pipe = pipe;

    this.transportExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.SYNC_SENDER_PIPE.getName() + "-" + pipe.getName());
    this.heartbeatExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.SYNC_SENDER_HEARTBEAT.getName() + "-" + pipe.getName());
  }

  public TransportClientMock getTransportClientMock() {
    return (TransportClientMock) transportClient;
  }
}
