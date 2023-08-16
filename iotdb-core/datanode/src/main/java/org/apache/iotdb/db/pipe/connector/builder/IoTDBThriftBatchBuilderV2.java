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

package org.apache.iotdb.db.pipe.connector.builder;

import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTabletBatchInsertionEventHandler;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class IoTDBThriftBatchBuilderV2 {
  private final AtomicLong batchCommitId = new AtomicLong(0);
  private final List<TPipeTransferReq> tPipeTransferReqs = new CopyOnWriteArrayList<>();
  private final List<Long> requestCommitIds = new CopyOnWriteArrayList<>();
  // TODO: replace it with List<EnrichedEvent> after PipeRawTabletInsertionEvent extending
  // EnrichedEvent
  private final List<Event> events = new CopyOnWriteArrayList<>();
  private final AtomicLong lastSendTime = new AtomicLong(0);
  private final AtomicLong bufferSize = new AtomicLong(0);

  private final int maxDelayInMs;
  private final long batchSizeInBytes;

  public IoTDBThriftBatchBuilderV2(int maxDelayInMs, long batchSizeInBytes) {
    this.maxDelayInMs = maxDelayInMs;
    this.batchSizeInBytes = batchSizeInBytes;
  }

  public List<Long> getRequestCommitIds() {
    return requestCommitIds;
  }

  public List<Event> getEvents() {
    return events;
  }

  public List<TPipeTransferReq> gettPipeTransferReqs() {
    return tPipeTransferReqs;
  }

  public AtomicLong getBatchCommitId() {
    return batchCommitId;
  }

  public void initLastSendTime() {
    if (lastSendTime.get() == 0) {
      lastSendTime.set(System.currentTimeMillis());
    }
  }

  public void tryCacheReq(
      TPipeTransferReq pipeTransferInsertNodeReq,
      Long requestCommitId,
      Event pipeInsertNodeTabletInsertionEvent) {
    tPipeTransferReqs.add(pipeTransferInsertNodeReq);
    requestCommitIds.add(requestCommitId);
    events.add(pipeInsertNodeTabletInsertionEvent);
    // TODO: remove the "if" after PipeRawTabletInsertionEvent extending EnrichedEvent
    if (pipeInsertNodeTabletInsertionEvent instanceof EnrichedEvent) {
      ((EnrichedEvent) pipeInsertNodeTabletInsertionEvent)
          .increaseReferenceCount(PipeTransferTabletBatchInsertionEventHandler.class.getName());
    }
    bufferSize.addAndGet(pipeTransferInsertNodeReq.body.array().length);
  }

  public boolean needSend() {
    return bufferSize.get() >= batchSizeInBytes
        || System.currentTimeMillis() - lastSendTime.get() >= maxDelayInMs;
  }

  public void clearBatchCaches() {
    tPipeTransferReqs.clear();
    requestCommitIds.clear();
    events.clear();
    bufferSize.set(0);
    lastSendTime.set(System.currentTimeMillis());
  }
}
