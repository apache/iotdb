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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.builder;

import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletReq;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTabletBatchInsertionEventHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBThriftSyncConnector;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PipeTransferBatchReqBuilder {

  private final List<TPipeTransferReq> tPipeTransferReqs = new ArrayList<>();
  private final List<EnrichedEvent> events = new ArrayList<>();
  private long lastSendTime = 0;
  private long bufferSize = 0;

  private final int maxDelayInMs;
  private final long batchSizeInBytes;

  private final List<Long> requestCommitIds = new ArrayList<>();
  private long batchCommitId = 0;

  public PipeTransferBatchReqBuilder(int maxDelayInMs, long batchSizeInBytes) {
    this.maxDelayInMs = maxDelayInMs;
    this.batchSizeInBytes = batchSizeInBytes;
  }

  public List<TPipeTransferReq> gettPipeTransferReqs() {
    return this.tPipeTransferReqs;
  }

  /////////////////////////////// Sync Interface ///////////////////////////////

  public boolean handleInsertionEventAndReturnNeedSend(TabletInsertionEvent event)
      throws IOException, WALPipeException {
    // Init last send time to record the first element
    initLastSendTime();
    tryCacheReq(
        event instanceof PipeInsertNodeTabletInsertionEvent
            ? PipeTransferInsertNodeReq.toTPipeTransferReq(
                ((PipeInsertNodeTabletInsertionEvent) event).getInsertNode())
            : PipeTransferTabletReq.toTPipeTransferReq(
                ((PipeRawTabletInsertionEvent) event).convertToTablet(),
                ((PipeRawTabletInsertionEvent) event).isAligned()),
        event);
    return needSend();
  }

  /////////////////////////////// Async Interface ///////////////////////////////

  public boolean handleInsertionEventAndReturnNeedSend(
      TabletInsertionEvent event, long requestCommitId) throws IOException, WALPipeException {
    // Init last send time to record the first element
    initLastSendTime();
    tryCacheReq(
        event instanceof PipeInsertNodeTabletInsertionEvent
            ? PipeTransferInsertNodeReq.toTPipeTransferReq(
                ((PipeInsertNodeTabletInsertionEvent) event).getInsertNode())
            : PipeTransferTabletReq.toTPipeTransferReq(
                ((PipeRawTabletInsertionEvent) event).convertToTablet(),
                ((PipeRawTabletInsertionEvent) event).isAligned()),
        requestCommitId,
        event);
    return needSend();
  }

  public List<Long> copyCommitIds() {
    return new ArrayList<>(requestCommitIds);
  }

  public List<Event> copyEvents() {
    return new ArrayList<>(events);
  }

  public long generateNextBatchId() {
    return batchCommitId++;
  }

  /////////////////////////////// Common Interface ///////////////////////////////

  public void clearBatchCaches() {
    tPipeTransferReqs.clear();
    bufferSize = 0;
    lastSendTime = System.currentTimeMillis();
    // Release the events to allow committing
    for (EnrichedEvent event : events) {
      event.decreaseReferenceCount(IoTDBThriftSyncConnector.class.getName());
    }
    events.clear();

    // Non-empty only in async mode
    requestCommitIds.clear();
  }

  /////////////////////////////// Private ///////////////////////////////

  private void initLastSendTime() {
    if (lastSendTime == 0) {
      lastSendTime = System.currentTimeMillis();
    }
  }

  // For IoTDBThriftSyncConnector
  private void tryCacheReq(TPipeTransferReq insertNodeReq, Event event) {
    if (events.isEmpty() || !events.get(events.size() - 1).equals(event)) {
      tPipeTransferReqs.add(insertNodeReq);
      // TODO: Remove the "if" after PipeRawTabletInsertionEvent extending EnrichedEvent
      if (event instanceof EnrichedEvent) {
        events.add((EnrichedEvent) event);
        ((EnrichedEvent) event).increaseReferenceCount(IoTDBThriftSyncConnector.class.getName());
      }
      bufferSize += insertNodeReq.body.array().length;
    }
  }

  // For IoTDBThriftAsyncConnector
  public void tryCacheReq(TPipeTransferReq insertNodeReq, Long requestCommitId, Event event) {
    if (events.isEmpty() || !events.get(events.size() - 1).equals(event)) {
      tPipeTransferReqs.add(insertNodeReq);
      requestCommitIds.add(requestCommitId);
      // TODO: remove the "if" after PipeRawTabletInsertionEvent extending EnrichedEvent
      if (event instanceof EnrichedEvent) {
        events.add((EnrichedEvent) event);
        ((EnrichedEvent) event)
            .increaseReferenceCount(PipeTransferTabletBatchInsertionEventHandler.class.getName());
      }
      bufferSize += insertNodeReq.body.array().length;
    }
  }

  private boolean needSend() {
    return bufferSize >= batchSizeInBytes
        || System.currentTimeMillis() - lastSendTime >= maxDelayInMs;
  }
}
