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

package org.apache.iotdb.db.pipe.sink.protocol.airgap;

import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSinkRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBDataRegionAirGapSinkTest {

  @Test
  public void testTransferTabletBatchOverAirGap() throws Exception {
    try (final RecordingIoTDBDataRegionAirGapSink sink = new RecordingIoTDBDataRegionAirGapSink()) {
      final PipeParameters parameters = buildParameters(false);
      sink.validate(new PipeParameterValidator(parameters));
      sink.customize(
          parameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskSinkRuntimeEnvironment("pipe", 1L, 1)));
      sink.prepareSocket();

      sink.transfer(createPipeRawTabletInsertionEvent("pipe", 1L, 1L));
      sink.transfer(createPipeRawTabletInsertionEvent("pipe", 1L, 2L));

      Thread.sleep(300L);
      sink.transfer(new PipeHeartbeatEvent(-1, false));

      Assert.assertEquals(1, sink.sentRequests.size());

      final TPipeTransferReq req = toTPipeTransferReq(sink.sentRequests.get(0));
      Assert.assertEquals(PipeRequestType.TRANSFER_TABLET_BATCH_V2.getType(), req.type);

      final PipeTransferTabletBatchReqV2 batchReq =
          PipeTransferTabletBatchReqV2.fromTPipeTransferReq(req);
      Assert.assertEquals(2, batchReq.getTabletReqs().size());
    }
  }

  @Test
  public void testTransferTsFileBatchOverAirGap() throws Exception {
    try (final RecordingIoTDBDataRegionAirGapSink sink = new RecordingIoTDBDataRegionAirGapSink()) {
      final PipeParameters parameters = buildParameters(true);
      sink.validate(new PipeParameterValidator(parameters));
      sink.customize(
          parameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskSinkRuntimeEnvironment("pipe", 1L, 1)));
      sink.prepareSocket();

      sink.transfer(createPipeRawTabletInsertionEvent("pipe", 1L, 1L));
      sink.transfer(createPipeRawTabletInsertionEvent("pipe", 1L, 2L));

      Thread.sleep(300L);
      sink.transfer(new PipeHeartbeatEvent(-1, false));

      final List<Short> requestTypes = new ArrayList<>();
      for (final byte[] requestBytes : sink.sentRequests) {
        requestTypes.add(toTPipeTransferReq(requestBytes).type);
      }

      Assert.assertTrue(requestTypes.contains(PipeRequestType.TRANSFER_TS_FILE_PIECE.getType()));
      Assert.assertTrue(
          requestTypes.contains(PipeRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD.getType()));
      Assert.assertFalse(requestTypes.contains(PipeRequestType.TRANSFER_TABLET_RAW_V2.getType()));
      Assert.assertFalse(requestTypes.contains(PipeRequestType.TRANSFER_TABLET_BATCH_V2.getType()));
    }
  }

  private PipeParameters buildParameters(final boolean useTsFileBatch) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSinkConstant.CONNECTOR_KEY, "iotdb-air-gap-connector");
    attributes.put(PipeSinkConstant.CONNECTOR_IOTDB_NODE_URLS_KEY, "127.0.0.1:6668");
    attributes.put(PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY, "200");
    attributes.put(PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY, "1048576");
    if (useTsFileBatch) {
      attributes.put(PipeSinkConstant.CONNECTOR_FORMAT_KEY, "tsfile");
    }
    return new PipeParameters(attributes);
  }

  private PipeRawTabletInsertionEvent createPipeRawTabletInsertionEvent(
      final String pipeName, final long creationTime, final long value) {
    final List<IMeasurementSchema> schemaList =
        Arrays.asList(new MeasurementSchema("s1", TSDataType.INT64));
    final Tablet tablet = new Tablet("root.db.d1", schemaList, 1);
    tablet.addTimestamp(0, value);
    tablet.addValue("s1", 0, value);
    return new PipeRawTabletInsertionEvent(
        false,
        "root.db",
        "db",
        "root.db",
        tablet,
        false,
        pipeName,
        creationTime,
        null,
        null,
        false);
  }

  private static TPipeTransferReq toTPipeTransferReq(final byte[] requestBytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(requestBytes);

    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = ReadWriteIOUtils.readByte(buffer);
    req.type = ReadWriteIOUtils.readShort(buffer);
    req.body = buffer.slice();
    return req;
  }

  private static class RecordingIoTDBDataRegionAirGapSink extends IoTDBDataRegionAirGapSink {

    private final List<byte[]> sentRequests = new ArrayList<>();

    private void prepareSocket() {
      sockets.set(0, new TestingAirGapSocket());
    }

    @Override
    protected int nextSocketIndex() {
      return 0;
    }

    @Override
    protected boolean sendBytes(final AirGapSocket socket, final byte[] bytes) {
      sentRequests.add(Arrays.copyOf(bytes, bytes.length));
      return true;
    }

    private static class TestingAirGapSocket extends AirGapSocket {

      private TestingAirGapSocket() {
        super("127.0.0.1", 6668);
      }

      @Override
      public synchronized void setSoTimeout(final int timeout) {
        // No-op for unit test.
      }
    }
  }
}
