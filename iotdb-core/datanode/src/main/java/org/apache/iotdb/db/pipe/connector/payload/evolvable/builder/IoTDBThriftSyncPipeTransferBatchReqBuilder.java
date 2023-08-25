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

import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBThriftSyncConnector;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;

public class IoTDBThriftSyncPipeTransferBatchReqBuilder extends PipeTransferBatchReqBuilder {

  public IoTDBThriftSyncPipeTransferBatchReqBuilder(PipeParameters parameters) {
    super(parameters);
  }

  /**
   * Try offer event into cache if the given event is not duplicated.
   *
   * @param event the given event
   * @return true if the batch can be transferred
   */
  public boolean onEvent(TabletInsertionEvent event) throws IOException, WALPipeException {
    final TPipeTransferReq req;

    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) event;
      if (!pipeInsertNodeTabletInsertionEvent
              .shouldParsePattern()
          && pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible() == null) {
        // we just need to read the bytebuffer from the wal file and transfer it directly without
        // serializing or deserializing
        req =
            PipeTransferTabletBinaryReq.toTPipeTransferReq(
                pipeInsertNodeTabletInsertionEvent.getByteBuffer());
      } else {
        req =
            PipeTransferTabletInsertNodeReq.toTPipeTransferReq(
                pipeInsertNodeTabletInsertionEvent.getInsertNode());
      }
    } else {
      req =
          PipeTransferTabletRawReq.toTPipeTransferReq(
              ((PipeRawTabletInsertionEvent) event).convertToTablet(),
              ((PipeRawTabletInsertionEvent) event).isAligned());
    }

    if (events.isEmpty() || !events.get(events.size() - 1).equals(event)) {
      reqs.add(req);

      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).increaseReferenceCount(PipeTransferBatchReqBuilder.class.getName());
      }
      events.add(event);

      if (firstEventProcessingTime == Long.MIN_VALUE) {
        firstEventProcessingTime = System.currentTimeMillis();
      }

      bufferSize += req.getBody().length;
    }

    return bufferSize >= maxBatchSizeInBytes
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  public void onSuccess() {
    reqs.clear();

    for (final Event event : events) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).decreaseReferenceCount(IoTDBThriftSyncConnector.class.getName());
      }
    }
    events.clear();

    firstEventProcessingTime = Long.MIN_VALUE;

    bufferSize = 0;
  }
}
