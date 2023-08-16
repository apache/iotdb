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

import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBThriftSyncConnector;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.util.ArrayList;
import java.util.List;

public class IoTDBThriftBatchBuilderV1 {
  private final List<TPipeTransferReq> tPipeTransferReqs = new ArrayList<>();
  private final List<EnrichedEvent> events = new ArrayList<>();
  private long lastSendTime = 0;
  private long bufferSize = 0;

  private final int maxDelayInMs;
  private final long batchSizeInBytes;

  public IoTDBThriftBatchBuilderV1(int maxDelayInMs, long batchSizeInBytes) {
    this.maxDelayInMs = maxDelayInMs;
    this.batchSizeInBytes = batchSizeInBytes;
  }

  public List<TPipeTransferReq> gettPipeTransferReqs() {
    return this.tPipeTransferReqs;
  }

  public void initLastSendTime() {
    if (lastSendTime == 0) {
      lastSendTime = System.currentTimeMillis();
    }
  }

  public void tryCacheReq(TPipeTransferReq insertNodeReq, Event event) {
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

  public boolean needSend() {
    return bufferSize >= batchSizeInBytes
        || System.currentTimeMillis() - lastSendTime >= maxDelayInMs;
  }

  public void clearBatchCaches() {
    tPipeTransferReqs.clear();
    bufferSize = 0;
    lastSendTime = System.currentTimeMillis();
    // Release the events to allow committing
    for (EnrichedEvent event : events) {
      event.decreaseReferenceCount(IoTDBThriftSyncConnector.class.getName());
    }
    events.clear();
  }
}
