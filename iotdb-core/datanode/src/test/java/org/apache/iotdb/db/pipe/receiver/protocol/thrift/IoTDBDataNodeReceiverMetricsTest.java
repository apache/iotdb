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

package org.apache.iotdb.db.pipe.receiver.protocol.thrift;

import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressor;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressorFactory;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.metric.receiver.PipeDataNodeReceiverMetrics;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBDataNodeReceiverMetricsTest {

  private final PipeDataNodeReceiverMetrics metrics = PipeDataNodeReceiverMetrics.getInstance();
  private final AbstractMetricService service = mock(AbstractMetricService.class, RETURNS_MOCKS);
  private final Rate handshakeRate = mock(Rate.class);
  private final Rate compressedRate = mock(Rate.class);

  @Before
  public void setUp() {
    when(service.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "pipeDataNodeReceiver",
            Tag.TYPE.toString(),
            "handshakeDataNodeV1"))
        .thenReturn(handshakeRate);
    when(service.getOrCreateRate(
            Metric.PIPE_DATANODE_RECEIVER_REQ_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "pipeDataNodeReceiver",
            Tag.TYPE.toString(),
            "transferCompressed"))
        .thenReturn(compressedRate);
    metrics.bindTo(service);
  }

  @After
  public void tearDown() {
    metrics.unbindFrom(service);
  }

  @Test
  public void testHandshakeByteCountExcludesBufferPrefix() throws Exception {
    final TPipeTransferReq req =
        PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq("unsupported-precision");
    final int bodySize = req.body.remaining();
    addBufferPrefix(req);

    // The precision mismatch exercises a handled failure without creating a transfer session.
    assertEquals(
        TSStatusCode.PIPE_HANDSHAKE_ERROR.getStatusCode(),
        new IoTDBDataNodeReceiver().receive(req).getStatus().getCode());
    verify(handshakeRate).mark(bodySize);
  }

  @Test
  public void testCompressedByteCountIsCapturedBeforeParsing() throws Exception {
    final TPipeTransferReq originalReq =
        PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq("unsupported-precision");
    final int originalBodySize = originalReq.body.remaining();
    final TPipeTransferReq req =
        PipeTransferCompressedReq.toTPipeTransferReq(
            originalReq,
            Collections.singletonList(
                PipeCompressorFactory.getCompressor(
                    PipeCompressor.PipeCompressionType.GZIP.getIndex())));
    final int compressedBodySize = req.body.remaining();
    addBufferPrefix(req);

    assertEquals(
        TSStatusCode.PIPE_HANDSHAKE_ERROR.getStatusCode(),
        new IoTDBDataNodeReceiver().receive(req).getStatus().getCode());
    assertEquals(0, req.body.remaining());
    verify(compressedRate).mark(compressedBodySize);
    verify(handshakeRate).mark(originalBodySize);
  }

  private static void addBufferPrefix(final TPipeTransferReq req) {
    final int prefixSize = 17;
    final int bodySize = req.body.remaining();
    final ByteBuffer frame = ByteBuffer.allocate(prefixSize + bodySize);
    frame.position(prefixSize);
    frame.put(req.body.duplicate());
    frame.flip();
    frame.position(prefixSize);
    req.body = frame.asReadOnlyBuffer();
  }
}
