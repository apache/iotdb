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

package org.apache.iotdb.db.pipe.connector.v2.handler;

import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.db.pipe.connector.v2.IoTDBThriftConnectorV2;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;

import javax.annotation.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class PipeTransferInsertNodeTabletInsertionEventHandler
    extends PipeTransferTabletInsertionEventHandler<TPipeTransferResp> {
  public PipeTransferInsertNodeTabletInsertionEventHandler(
      long requestCommitId,
      @Nullable EnrichedEvent event,
      TPipeTransferReq req,
      IoTDBThriftConnectorV2 connector,
      ExecutorService retryExecutor,
      BlockingQueue<Pair<Long, Event>> retryFailureQueue) {
    super(requestCommitId, event, req, connector, retryExecutor, retryFailureQueue);
  }

  @Override
  protected void doTransfer(AsyncPipeDataTransferServiceClient client, TPipeTransferReq req)
      throws TException {
    client.pipeTransfer(req, this);
  }

  @Override
  protected void retryTransfer(IoTDBThriftConnectorV2 connector, long requestCommitId) {
    connector.transfer(requestCommitId, this);
  }
}
