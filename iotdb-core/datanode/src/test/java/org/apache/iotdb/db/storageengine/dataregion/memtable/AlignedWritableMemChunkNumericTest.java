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

import org.apache.iotdb.db.utils.NumericRowValues;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AlignedWritableMemChunkNumericTest {

  @Test
  public void testMappingAllocatedOnlyForNumericRowsAndReused() throws Exception {
    // Ordinary row/Tablet writes must not allocate scratch space; numeric rows share it.
    List<IMeasurementSchema> schemas =
        Collections.singletonList(new MeasurementSchema("a", TSDataType.INT64));
    AlignedWritableMemChunk chunk = new AlignedWritableMemChunk(new ArrayList<>(schemas), true);
    try {
      assertNull(mapping(chunk));
      chunk.writeAlignedPoints(1, new Object[] {10L}, schemas);
      chunk.writeAlignedTablet(
          new long[] {2}, new Object[] {new long[] {20}}, null, schemas, 0, 1, null);
      assertNull(mapping(chunk));

      chunk.writeAlignedNumericPoints(3, row(30), schemas);
      int[] mapping = mapping(chunk);
      assertNotNull(mapping);
      chunk.writeAlignedNumericPoints(4, row(40), schemas);
      assertSame(mapping, mapping(chunk));
      assertEquals(30, chunk.getWorkingTVList().getLongByValueIndex(2, 0));
      assertEquals(40, chunk.getWorkingTVList().getLongByValueIndex(3, 0));
    } finally {
      chunk.release();
    }
  }

  @Test
  public void testMappingResetAndGrowthWithMixedWrites() throws Exception {
    // Reordered/absent columns must stay correct after reuse and schema growth by either API.
    IMeasurementSchema a = new MeasurementSchema("a", TSDataType.INT64);
    IMeasurementSchema b = new MeasurementSchema("b", TSDataType.INT64);
    IMeasurementSchema c = new MeasurementSchema("c", TSDataType.INT64);
    IMeasurementSchema d = new MeasurementSchema("d", TSDataType.INT64);
    AlignedWritableMemChunk chunk =
        new AlignedWritableMemChunk(new ArrayList<>(Arrays.asList(a, b)), true);
    try {
      chunk.writeAlignedNumericPoints(1, row(20, 10), Arrays.asList(b, a));
      int[] mapping = mapping(chunk);
      chunk.writeAlignedNumericPoints(2, row(30, 999), Arrays.asList(a, null));
      assertSame(mapping, mapping(chunk));
      AlignedTVList list = chunk.getWorkingTVList();
      assertEquals(10, list.getLongByValueIndex(0, 0));
      assertEquals(20, list.getLongByValueIndex(0, 1));
      assertEquals(30, list.getLongByValueIndex(1, 0));
      assertTrue(list.isNullValue(1, 1));

      chunk.writeAlignedNumericPoints(3, row(40, 50), Arrays.asList(c, b));
      int[] extendedMapping = mapping(chunk);
      assertEquals(3, extendedMapping.length);
      assertTrue(list.isNullValue(2, 0));
      assertEquals(50, list.getLongByValueIndex(2, 1));
      assertEquals(40, list.getLongByValueIndex(2, 2));

      chunk.writeAlignedTablet(
          new long[] {4},
          new Object[] {new long[] {60}},
          null,
          Collections.singletonList(d),
          0,
          1,
          null);
      assertSame(extendedMapping, mapping(chunk));
      chunk.writeAlignedNumericPoints(5, row(70), Collections.singletonList(a));
      assertEquals(4, mapping(chunk).length);
      assertEquals(60, list.getLongByValueIndex(3, 3));
      assertEquals(70, list.getLongByValueIndex(4, 0));
      assertTrue(list.isNullValue(4, 1));
      assertTrue(list.isNullValue(4, 2));
      assertTrue(list.isNullValue(4, 3));

      mapping = mapping(chunk);
      chunk.writeAlignedNumericPoints(6, row(80), Collections.singletonList(d));
      assertSame(mapping, mapping(chunk));
      assertTrue(list.isNullValue(5, 0));
      assertEquals(80, list.getLongByValueIndex(5, 3));
    } finally {
      chunk.release();
    }
  }

  private static NumericRowValues row(long... values) {
    NumericRowValues row = new NumericRowValues(0, values.length);
    for (int i = 0; i < values.length; i++) {
      row.setRawValue(i, values[i]);
    }
    return row;
  }

  private static int[] mapping(AlignedWritableMemChunk chunk) throws Exception {
    // Array identity verifies the allocation contract without exposing scratch state as public API.
    Field field = AlignedWritableMemChunk.class.getDeclaredField("numericColumnMapping");
    field.setAccessible(true);
    return (int[]) field.get(chunk);
  }
}
