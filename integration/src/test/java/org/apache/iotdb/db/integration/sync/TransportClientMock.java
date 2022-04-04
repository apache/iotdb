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

import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.transport.client.ITransportClient;
import org.apache.iotdb.service.transport.thrift.ResponseType;
import org.apache.iotdb.service.transport.thrift.SyncRequest;
import org.apache.iotdb.service.transport.thrift.SyncResponse;

import java.util.ArrayList;
import java.util.List;

public class TransportClientMock implements ITransportClient {
  private final Pipe pipe;
  private final String ipAddress;
  private final int port;

  private List<PipeData> pipeDataList;

  public TransportClientMock(Pipe pipe, String ipAddress, int port) {
    this.pipe = pipe;
    this.ipAddress = ipAddress;
    this.port = port;

    this.pipeDataList = new ArrayList<>();
  }

  @Override
  public SyncResponse heartbeat(SyncRequest syncRequest) throws SyncConnectionException {
    return new SyncResponse(ResponseType.INFO, "");
  }

  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        PipeData pipeData = pipe.take();
        pipeDataList.add(pipeData);
        pipe.commit();
      }
    } catch (InterruptedException e) {
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public List<PipeData> getPipeDataList() {
    return pipeDataList;
  }
}
