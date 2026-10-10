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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AlignedWritableMemChunkTest {

  @Test
  public void testTabletExtendsColumnsWithReorderingAndBitmaps() throws IOException {
    IMeasurementSchema existing = new MeasurementSchema("existing", TSDataType.INT64);
    IMeasurementSchema omitted = new MeasurementSchema("omitted", TSDataType.BOOLEAN);
    IMeasurementSchema integer = new MeasurementSchema("integer", TSDataType.INT32);
    IMeasurementSchema text = new MeasurementSchema("text", TSDataType.TEXT);
    List<IMeasurementSchema> initialSchemas = Arrays.asList(existing, omitted);
    AlignedWritableMemChunk chunk =
        new AlignedWritableMemChunk(new ArrayList<>(initialSchemas), false);
    chunk.writeAlignedPoints(0, new Object[] {1L, true}, initialSchemas);
    Binary binary = new Binary("value", TSFileConfig.STRING_CHARSET);
    BitMap integerBitmap = new BitMap(3);
    integerBitmap.mark(2);
    BitMap textBitmap = new BitMap(3);
    textBitmap.mark(1);
    chunk.writeAlignedTablet(
        new long[] {-1, 1, 2},
        new Object[] {
          new int[] {0, 1, 2},
          new long[] {0, 10, 20},
          null,
          new Binary[] {binary, binary, binary},
          new int[] {0, 11, 12}
        },
        new BitMap[] {null, null, null, textBitmap, integerBitmap},
        Arrays.asList(integer, existing, null, text, integer),
        1,
        3,
        null);

    Assert.assertEquals(Arrays.asList(existing, omitted, integer, text), chunk.getSchemaList());
    Assert.assertEquals(2, chunk.getMeasurementIndex("integer"));
    Assert.assertEquals(3, chunk.getMeasurementIndex("text"));
    AlignedTVList list = chunk.getWorkingTVList();
    Assert.assertEquals(3, list.rowCount());
    Assert.assertTrue(list.isNullValue(0, 2));
    Assert.assertTrue(list.isNullValue(0, 3));
    Assert.assertTrue(list.isNullValue(1, 1));
    Assert.assertTrue(list.isNullValue(2, 1));
    Assert.assertEquals(10L, list.getLongByValueIndex(1, 0));
    Assert.assertEquals(20L, list.getLongByValueIndex(2, 0));
    Assert.assertEquals(11, list.getIntByValueIndex(1, 2));
    Assert.assertTrue(list.isNullValue(2, 2));
    Assert.assertTrue(list.isNullValue(1, 3));
    Assert.assertEquals(binary, list.getBinaryByValueIndex(2, 3));

    WALByteBufferForTest buffer =
        new WALByteBufferForTest(ByteBuffer.allocate(chunk.serializedSize()));
    chunk.serializeToWAL(buffer);
    AlignedWritableMemChunk restored =
        AlignedWritableMemChunk.deserialize(
            new DataInputStream(new ByteArrayInputStream(buffer.getBuffer().array())), false);
    Assert.assertEquals(chunk.getSchemaList(), restored.getSchemaList());
    AlignedTVList restoredList = restored.getWorkingTVList();
    for (int row = 0; row < list.rowCount(); row++) {
      Assert.assertEquals(list.getTime(row), restoredList.getTime(row));
      Assert.assertEquals(
          list.getAlignedValue(row).toString(), restoredList.getAlignedValue(row).toString());
    }
    chunk.release();
    restored.release();
  }

  @Test
  public void testPointsExtendColumnsAfterRemovalAndHandover() {
    IMeasurementSchema original = new MeasurementSchema("original", TSDataType.INT64);
    IMeasurementSchema added = new MeasurementSchema("added", TSDataType.INT32);
    AlignedWritableMemChunk chunk =
        new AlignedWritableMemChunk(new ArrayList<>(Collections.singletonList(original)), false);
    chunk.writeAlignedPoints(0, new Object[] {1L}, Collections.singletonList(original));
    chunk.removeColumn("original");
    chunk.writeAlignedPoints(1, new Object[] {7, null, 2L}, Arrays.asList(added, null, original));

    Assert.assertEquals(3, chunk.getSchemaList().size());
    Assert.assertEquals(1, chunk.getMeasurementIndex("added"));
    Assert.assertEquals(2, chunk.getMeasurementIndex("original"));
    AlignedTVList list = chunk.getWorkingTVList();
    Assert.assertTrue(list.isNullValue(0, 1));
    Assert.assertTrue(list.isNullValue(0, 2));
    Assert.assertEquals(7, list.getIntByValueIndex(1, 1));
    Assert.assertEquals(2L, list.getLongByValueIndex(1, 2));
    chunk.handoverAlignedTvList();

    IMeasurementSchema afterHandover = new MeasurementSchema("after", TSDataType.DOUBLE);
    chunk.writeAlignedPoints(
        2, new Object[] {3.5, 3L, 8}, Arrays.asList(afterHandover, original, added));
    Assert.assertEquals(3, chunk.getMeasurementIndex("after"));
    Assert.assertEquals(3, chunk.getSortedList().get(0).getTsDataTypes().size());
    Assert.assertEquals(4, chunk.getWorkingTVList().getTsDataTypes().size());
    Assert.assertEquals(8, chunk.getWorkingTVList().getIntByValueIndex(0, 1));
    Assert.assertEquals(3L, chunk.getWorkingTVList().getLongByValueIndex(0, 2));
    Assert.assertEquals(3.5, chunk.getWorkingTVList().getDoubleByValueIndex(0, 3), 0);
    chunk.release();
  }

  @Test
  public void testWideTabletKeepsUnwrittenColumnsLazy() {
    int columnCount = 10_000;
    IMeasurementSchema existing = new MeasurementSchema("existing", TSDataType.INT64);
    AlignedWritableMemChunk chunk =
        new AlignedWritableMemChunk(new ArrayList<>(Collections.singletonList(existing)), false);
    chunk.writeAlignedPoints(0, new Object[] {1L}, Collections.singletonList(existing));
    List<IMeasurementSchema> incomingSchemas = new ArrayList<>(columnCount);
    Object[] values = new Object[columnCount];
    for (int column = 0; column < columnCount; column++) {
      incomingSchemas.add(new MeasurementSchema("s" + column, TSDataType.INT64));
    }
    values[0] = new long[] {2L};
    values[columnCount - 1] = new long[] {3L};
    chunk.writeAlignedTablet(new long[] {1}, values, null, incomingSchemas, 0, 1, null);

    AlignedTVList list = chunk.getWorkingTVList();
    Assert.assertEquals(columnCount + 1, list.getTsDataTypes().size());
    Assert.assertEquals(columnCount + 1, chunk.getSchemaList().size());
    Assert.assertEquals(2, list.rowCount());
    Assert.assertEquals(2L, list.getLongByValueIndex(1, 1));
    Assert.assertEquals(3L, list.getLongByValueIndex(1, columnCount));
    for (int column = 1; column <= columnCount; column++) {
      Assert.assertTrue(list.isNullValue(0, column));
      Assert.assertEquals(column, chunk.getMeasurementIndex("s" + (column - 1)));
      if (column > 1 && column < columnCount) {
        Assert.assertNull(list.getValues().get(column).get(0));
        Assert.assertTrue(list.isNullValue(1, column));
      }
    }
    chunk.writeAlignedPoints(
        2,
        new Object[] {4L, 5L},
        Arrays.asList(incomingSchemas.get(columnCount - 1), incomingSchemas.get(0)));
    Assert.assertEquals(columnCount + 1, chunk.getSchemaList().size());
    Assert.assertEquals(5L, list.getLongByValueIndex(2, 1));
    Assert.assertEquals(4L, list.getLongByValueIndex(2, columnCount));
    chunk.release();
  }
}
