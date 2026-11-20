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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReqV2;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipeTabletEventPlainBatchSerializationTest {

  @Test
  public void testTreeBatchMatchesStreamWireFormat() throws Exception {
    final Tablet aligned = tablet("root.test.aligned");
    final Tablet nonAligned = tablet("root.test.non_aligned");
    try (final PipeTabletEventPlainBatch batch =
        new PipeTabletEventPlainBatch(1000, 1024 * 1024, null)) {
      batch.constructBatch(event(aligned, false, true));
      batch.constructBatch(event(nonAligned, false, false));
      final ByteBuffer expected =
          PipeTransferTabletBatchReqV2.toTPipeTransferReq(
                  Collections.emptyList(),
                  Arrays.asList(streamBytes(aligned, true), streamBytes(nonAligned, false)),
                  Collections.emptyList(),
                  Arrays.asList("root.test", "root.test"))
              .body;
      Assert.assertEquals(expected, batch.toTPipeTransferReq().body);
    }
  }

  @Test
  public void testMergedTableBatchMatchesStreamWireFormat() throws Exception {
    final Tablet first = tablet("table1");
    final Tablet second = tablet("table1");
    final Tablet expectedTablet = tablet("table1");
    Assert.assertTrue(expectedTablet.append(second));
    try (final PipeTabletEventPlainBatch batch =
        new PipeTabletEventPlainBatch(1000, 1024 * 1024, null)) {
      batch.constructBatch(event(first, true, true));
      batch.constructBatch(event(second, true, true));
      final ByteBuffer expected =
          PipeTransferTabletBatchReqV2.toTPipeTransferReq(
                  Collections.emptyList(),
                  Collections.singletonList(streamBytes(expectedTablet, true)),
                  Collections.emptyList(),
                  Collections.singletonList("test"))
              .body;
      Assert.assertEquals(expected, batch.toTPipeTransferReq().body);
      Assert.assertEquals(3, first.getRowSize());
      Assert.assertEquals(3, second.getRowSize());
    }
  }

  private static PipeRawTabletInsertionEvent event(
      final Tablet tablet, final boolean tableModel, final boolean aligned) {
    final PipeRawTabletInsertionEvent event = mock(PipeRawTabletInsertionEvent.class);
    when(event.convertToTablet()).thenReturn(tablet);
    when(event.isTableModelEvent()).thenReturn(tableModel);
    when(event.isAligned()).thenReturn(aligned);
    when(event.getTreeModelDatabaseName()).thenReturn("root.test");
    when(event.getTableModelDatabaseName()).thenReturn("test");
    return event;
  }

  private static ByteBuffer streamBytes(final Tablet tablet, final boolean aligned)
      throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final DataOutputStream stream = new DataOutputStream(bytes)) {
      tablet.serialize(stream);
      ReadWriteIOUtils.write(aligned, stream);
    }
    return ByteBuffer.wrap(bytes.toByteArray());
  }

  private static Tablet tablet(final String name) {
    final TSDataType[] types = {
      TSDataType.BOOLEAN,
      TSDataType.INT32,
      TSDataType.INT64,
      TSDataType.TIMESTAMP,
      TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.DATE,
      TSDataType.TEXT,
      TSDataType.STRING,
      TSDataType.BLOB,
      TSDataType.OBJECT
    };
    final Object[] values = {
      true,
      Integer.MIN_VALUE,
      Long.MAX_VALUE,
      -1L,
      Float.intBitsToFloat(0x7f800001),
      Double.longBitsToDouble(0x7ff0000000000001L),
      LocalDate.of(2024, 2, 29),
      new Binary(new byte[] {0, 1, -1}),
      new Binary(new byte[] {2, 3}),
      Binary.EMPTY_VALUE,
      new Binary(new byte[] {4, 5})
    };
    final List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int column = 0; column < types.length; column++) {
      schemas.add(new MeasurementSchema("s" + column, types[column]));
    }
    final Tablet tablet = new Tablet(name, schemas, 5);
    for (int row = 0; row < 3; row++) {
      tablet.addTimestamp(row, row);
      for (int column = 0; column < types.length; column++) {
        tablet.addValue("s" + column, row, row == 1 && column % 2 == 0 ? null : values[column]);
      }
    }
    tablet.addValue(2, 4, -0.0f);
    tablet.addValue(2, 5, -0.0d);
    return tablet;
  }
}
