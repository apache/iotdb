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

package org.apache.iotdb.db.queryengine.transformation.dag.adapter;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.utils.RowImpl;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RowGetDoubleTest {

  @Test
  public void testNumericConversion() throws IOException {
    // Both Object[] implementations must widen all four numeric types, including large longs.
    for (Row row :
        createRows(
            new TSDataType[] {
              TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
            },
            new Object[] {Integer.MIN_VALUE, Long.MAX_VALUE, 0.1f, -1.25d, 123L})) {
      assertEquals((double) Integer.MIN_VALUE, row.getDouble(0), 0d);
      assertEquals((double) Long.MAX_VALUE, row.getDouble(1), 0d);
      assertEquals((double) 0.1f, row.getDouble(2), 0d);
      assertEquals(-1.25d, row.getDouble(3), 0d);
      assertEquals(123L, row.getTime());
      assertEquals(4, row.size());
    }
  }

  @Test
  public void testSpecialFloatingPointValues() throws IOException {
    // Widening must preserve NaN, infinities, and the sign of zero.
    for (Row row :
        createRows(
            new TSDataType[] {
              TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.DOUBLE
            },
            new Object[] {
              Float.NaN, Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0d, 123L
            })) {
      assertTrue(Double.isNaN(row.getDouble(0)));
      assertEquals(Double.POSITIVE_INFINITY, row.getDouble(1), 0d);
      assertEquals(Double.NEGATIVE_INFINITY, row.getDouble(2), 0d);
      assertEquals(Double.doubleToLongBits(-0.0d), Double.doubleToLongBits(row.getDouble(3)));
    }
  }

  @Test
  public void testInvalidReads() throws IOException {
    // Null, nonnumeric values, and invalid array indices retain their failure behavior.
    for (Row row :
        createRows(
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.BOOLEAN},
            new Object[] {null, true, 123L})) {
      assertTrue(row.isNull(0));
      assertThrows(NullPointerException.class, () -> row.getDouble(0));
      assertThrows(ClassCastException.class, () -> row.getDouble(1));
      assertThrows(IndexOutOfBoundsException.class, () -> row.getDouble(-1));
      assertThrows(IndexOutOfBoundsException.class, () -> row.getDouble(3));
    }
  }

  private List<Row> createRows(TSDataType[] types, Object[] values) {
    // RowImpl includes the trailing timestamp in its schema; the adapter does not.
    TSDataType[] typesWithTime = Arrays.copyOf(types, types.length + 1);
    typesWithTime[types.length] = TSDataType.TIMESTAMP;
    RowImpl row = new RowImpl(typesWithTime);
    row.setRowRecord(values);
    return Arrays.asList(
        row, new ElasticSerializableRowRecordListBackedMultiColumnRow(types).setRowRecord(values));
  }
}
