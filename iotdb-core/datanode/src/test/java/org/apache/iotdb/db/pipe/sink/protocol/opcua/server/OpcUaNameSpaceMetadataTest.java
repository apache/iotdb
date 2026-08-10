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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.pipe.sink.protocol.opcua.server;

import org.apache.iotdb.db.pipe.sink.protocol.opcua.OpcUaSink;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class OpcUaNameSpaceMetadataTest {

  @Test
  public void testTransferLastValuesForTreeModel() throws Exception {
    final CapturedRow capturedRow = new CapturedRow();

    OpcUaNameSpace.transferLastValues(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1"),
        Arrays.asList(
            lastValue("s1", TSDataType.INT64, 5L, 50L),
            lastValue("s2", TSDataType.TEXT, 6L, "last"),
            new Pair<>(new MeasurementSchema("empty", TSDataType.INT64), null),
            new Pair<>(
                new MeasurementSchema(TsFileConstant.TIME_COLUMN_ID, TSDataType.INT64),
                timeValue(TSDataType.INT64, 7L, 8L))),
        false,
        createSink(),
        capturedRow::capture);

    Assert.assertArrayEquals(new String[] {"root", "sg", "d1"}, capturedRow.segments.get());
    Assert.assertEquals(Arrays.asList("s1", "s2"), capturedRow.getMeasurementNames());
    Assert.assertEquals(Arrays.asList(5L, 6L), capturedRow.timestamps.get());
    Assert.assertEquals(Arrays.asList(50L, "last"), capturedRow.values.get());
  }

  @Test
  public void testTransferLastValuesForTableModel() throws Exception {
    final int lastDate = DateUtils.parseDateExpressionToInt(java.time.LocalDate.of(2024, 1, 2));
    final long lastTimestamp = 1_700_000_001_000L;
    final CapturedRow capturedRow = new CapturedRow();
    final OpcUaSink sink = createSink();
    Mockito.when(sink.getDatabaseName()).thenReturn("database");
    Mockito.when(sink.getPlaceHolder4NullTag()).thenReturn("null_tag");

    OpcUaNameSpace.transferLastValues(
        new StringArrayDeviceID("table", "tag", null, "tag2"),
        Arrays.asList(
            lastValue("date", TSDataType.DATE, 2L, lastDate),
            lastValue("timestamp", TSDataType.TIMESTAMP, 4L, lastTimestamp)),
        true,
        sink,
        capturedRow::capture);

    Assert.assertArrayEquals(
        new String[] {"database", "table", "tag", "null_tag", "tag2"}, capturedRow.segments.get());
    Assert.assertEquals(Arrays.asList("date", "timestamp"), capturedRow.getMeasurementNames());
    Assert.assertEquals(Arrays.asList(2L, 4L), capturedRow.timestamps.get());
    Assert.assertEquals(
        new DateTime(new java.util.Date(DateUtils.parseIntToDate(lastDate).getTime())).getUtcTime(),
        ((DateTime) capturedRow.values.get().get(0)).getUtcTime());
    Assert.assertEquals(
        OpcUaNameSpace.timestampToUtc(lastTimestamp),
        ((DateTime) capturedRow.values.get().get(1)).getUtcTime());
  }

  @Test
  public void testTransferLastValuesSupportsBinaryValues() throws Exception {
    final CapturedRow capturedRow = new CapturedRow();

    OpcUaNameSpace.transferLastValues(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1"),
        Arrays.asList(lastValue("blob", TSDataType.BLOB, 1L, "payload")),
        false,
        createSink(),
        capturedRow::capture);

    Assert.assertEquals(Arrays.asList("blob"), capturedRow.getMeasurementNames());
    Assert.assertEquals(TSDataType.BLOB, capturedRow.schemas.get().get(0).getType());
    Assert.assertEquals(Arrays.asList("payload"), capturedRow.values.get());
  }

  private static OpcUaSink createSink() {
    final OpcUaSink sink = Mockito.mock(OpcUaSink.class);
    Mockito.when(sink.getPlaceHolder4NullTag()).thenReturn("null");
    return sink;
  }

  private static Pair<IMeasurementSchema, TimeValuePair> lastValue(
      final String measurement,
      final TSDataType dataType,
      final long timestamp,
      final Object value) {
    return new Pair<>(
        new MeasurementSchema(measurement, dataType), timeValue(dataType, timestamp, value));
  }

  private static TimeValuePair timeValue(
      final TSDataType dataType, final long timestamp, final Object value) {
    final Object primitiveValue =
        dataType == TSDataType.TEXT || dataType == TSDataType.BLOB || dataType == TSDataType.STRING
            ? new Binary(
                String.valueOf(value), org.apache.tsfile.common.conf.TSFileConfig.STRING_CHARSET)
            : value;
    return new TimeValuePair(timestamp, TsPrimitiveType.getByType(dataType, primitiveValue));
  }

  private static class CapturedRow {
    private final AtomicReference<String[]> segments = new AtomicReference<>();
    private final AtomicReference<List<IMeasurementSchema>> schemas = new AtomicReference<>();
    private final AtomicReference<List<Long>> timestamps = new AtomicReference<>();
    private final AtomicReference<List<Object>> values = new AtomicReference<>();

    private void capture(
        final String[] segments,
        final List<IMeasurementSchema> schemas,
        final List<Long> timestamps,
        final List<Object> values,
        final OpcUaSink sink) {
      this.segments.set(segments);
      this.schemas.set(new ArrayList<>(schemas));
      this.timestamps.set(new ArrayList<>(timestamps));
      this.values.set(new ArrayList<>(values));
    }

    private List<String> getMeasurementNames() {
      return schemas.get().stream()
          .map(IMeasurementSchema::getMeasurementName)
          .collect(Collectors.toList());
    }
  }
}
