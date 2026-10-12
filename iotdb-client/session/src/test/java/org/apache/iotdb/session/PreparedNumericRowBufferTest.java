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

package org.apache.iotdb.session;

import org.apache.iotdb.isession.NumericRowPreparedStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PreparedNumericRowBufferTest {
  @Test
  public void testAllNumericTypesPreserveRawBits() throws Exception {
    Session session = mock(Session.class);
    SessionConnection connection = mock(SessionConnection.class);
    when(session.getDefaultSessionConnection()).thenReturn(connection);
    when(connection.prepareNumericRow(any())).thenReturn(1L);
    TableSession tableSession = new TableSession(session);
    NumericRowPreparedStatement prepared =
        tableSession.prepareInsertNumericRow(
            "t",
            List.of("b", "i", "l", "f", "d", "date", "ts"),
            List.of(
                TSDataType.BOOLEAN,
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.DOUBLE,
                TSDataType.DATE,
                TSDataType.TIMESTAMP),
            Collections.nCopies(7, ColumnCategory.FIELD));
    doAnswer(
            invocation -> {
              ByteBuffer values = invocation.getArgument(2);
              assertEquals(37, values.remaining());
              assertEquals(1, values.get());
              assertEquals(Integer.MIN_VALUE, values.getInt());
              assertEquals(Long.MIN_VALUE, values.getLong());
              assertEquals(0x7fc01234, values.getInt());
              assertEquals(0x7ff8000000001234L, values.getLong());
              assertEquals(20260910, values.getInt());
              assertEquals(Long.MAX_VALUE, values.getLong());
              assertFalse(values.hasRemaining());
              return null;
            })
        .when(connection)
        .executePreparedNumericRow(eq(1L), anyLong(), any(ByteBuffer.class), any(ByteBuffer.class));
    // High bits of 32-bit fields are ignored; noncanonical NaNs must survive without normalization.
    tableSession.executePreparedInsertNumericRow(
        prepared,
        42,
        new long[] {
          -1,
          0x1234567880000000L,
          Long.MIN_VALUE,
          0x123456787fc01234L,
          0x7ff8000000001234L,
          0x1234567800000000L | 20260910,
          Long.MAX_VALUE
        },
        new String[0]);
    verify(connection)
        .executePreparedNumericRow(eq(1L), eq(42L), any(ByteBuffer.class), any(ByteBuffer.class));
  }

  // Reusing the statement after sparse/all-null rows must reset both payload and bitmap cursors.
  @Test
  public void testNullBitmapBufferReuse() throws Exception {
    Session session = mock(Session.class);
    SessionConnection connection = mock(SessionConnection.class);
    when(session.getDefaultSessionConnection()).thenReturn(connection);
    when(connection.prepareNumericRow(any())).thenReturn(1L);
    TableSession tableSession = new TableSession(session);
    NumericRowPreparedStatement prepared =
        tableSession.prepareInsertNumericRow(
            "t",
            List.of("a", "b", "c"),
            List.of(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
            List.of(ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD));
    List<ByteBuffer> bitmaps = new ArrayList<>();
    List<ByteBuffer> payloads = new ArrayList<>();
    int[] masks = {2, 7, 0};
    doAnswer(
            invocation -> {
              int index = bitmaps.size();
              ByteBuffer values = invocation.getArgument(2);
              ByteBuffer bitmap = invocation.getArgument(4);
              bitmaps.add(bitmap);
              payloads.add(values);
              assertEquals(masks[index], bitmap.get());
              assertFalse(bitmap.hasRemaining());
              for (int field = 0; field < 3; field++) {
                if ((masks[index] & (1 << field)) == 0) {
                  assertEquals(field + 10, values.getLong());
                }
              }
              assertFalse(values.hasRemaining());
              return null;
            })
        .when(connection)
        .executePreparedNumericRow(
            eq(1L), anyLong(), any(ByteBuffer.class), any(ByteBuffer.class), any(ByteBuffer.class));
    BitMap nulls = new BitMap(3);
    for (int mask : masks) {
      nulls.reset();
      for (int i = 0; i < 3; i++) {
        if ((mask & (1 << i)) != 0) {
          nulls.mark(i);
        }
      }
      tableSession.executePreparedInsertNumericRow(
          prepared, 1, new long[] {10, 11, 12}, new String[0], nulls);
    }
    assertEquals(3, bitmaps.size());
    for (int i = 1; i < 3; i++) {
      assertSame(bitmaps.get(0), bitmaps.get(i));
      assertSame(payloads.get(0), payloads.get(i));
    }
  }

  @Test
  public void testBuffersAcrossTagGrowthShrinkAndRpcFailure() throws Exception {
    Session session = mock(Session.class);
    SessionConnection connection = mock(SessionConnection.class);
    when(session.getDefaultSessionConnection()).thenReturn(connection);
    when(connection.prepareNumericRow(any())).thenReturn(1L);
    TableSession tableSession = new TableSession(session);
    NumericRowPreparedStatement prepared =
        tableSession.prepareInsertNumericRow(
            "t",
            List.of("tag", "flag", "count", "value"),
            List.of(TSDataType.STRING, TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.DOUBLE),
            List.of(
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD));
    List<ByteBuffer> tagBuffers = new ArrayList<>();
    List<ByteBuffer> valueBuffers = new ArrayList<>();
    String[] tags = {"x", "long-tag-\u4e2d\u6587", "", null, "retry"};
    doAnswer(
            invocation -> {
              int index = tagBuffers.size();
              ByteBuffer values = invocation.getArgument(2);
              ByteBuffer encodedTags = invocation.getArgument(3);
              tagBuffers.add(encodedTags);
              valueBuffers.add(values);
              // Consume both buffers as a transport may do, and verify no previous row bytes
              // remain.
              assertEquals(tags[index], ReadWriteIOUtils.readString(encodedTags));
              assertFalse(encodedTags.hasRemaining());
              assertEquals(13, values.remaining());
              assertEquals((byte) (index % 2), values.get());
              assertEquals(index, values.getInt());
              assertEquals(Double.doubleToRawLongBits(index + 0.5), values.getLong());
              assertFalse(values.hasRemaining());
              if (index == 3) {
                throw new IoTDBConnectionException("simulated transport failure");
              }
              return null;
            })
        .when(connection)
        .executePreparedNumericRow(eq(1L), anyLong(), any(ByteBuffer.class), any(ByteBuffer.class));
    for (int i = 0; i < tags.length; i++) {
      try {
        tableSession.executePreparedInsertNumericRow(
            prepared,
            i,
            new long[] {i % 2, i, Double.doubleToRawLongBits(i + 0.5)},
            new String[] {tags[i]});
        assertNotEquals(3, i);
      } catch (IoTDBConnectionException e) {
        assertEquals(3, i);
      }
    }
    assertEquals(tags.length, tagBuffers.size());
    assertNotSame(tagBuffers.get(0), tagBuffers.get(1));
    for (int i = 1; i < tags.length; i++) {
      assertSame(valueBuffers.get(0), valueBuffers.get(i));
      assertSame(tagBuffers.get(1), tagBuffers.get(i));
    }
  }

  @Test
  public void testBuffersAreOwnedByEachStatementWithoutTags() throws Exception {
    Session session = mock(Session.class);
    SessionConnection connection = mock(SessionConnection.class);
    when(session.getDefaultSessionConnection()).thenReturn(connection);
    when(connection.prepareNumericRow(any())).thenReturn(0L, 1L);
    TableSession tableSession = new TableSession(session);
    List<ByteBuffer> buffers = new ArrayList<>();
    doAnswer(
            invocation -> {
              ByteBuffer values = invocation.getArgument(2);
              ByteBuffer tags = invocation.getArgument(3);
              assertEquals(0, tags.remaining());
              assertEquals(42L, values.getLong());
              assertFalse(values.hasRemaining());
              buffers.add(values);
              buffers.add(tags);
              return null;
            })
        .when(connection)
        .executePreparedNumericRow(
            anyLong(), anyLong(), any(ByteBuffer.class), any(ByteBuffer.class));
    for (int i = 0; i < 2; i++) {
      NumericRowPreparedStatement prepared =
          tableSession.prepareInsertNumericRow(
              "t", List.of("v"), List.of(TSDataType.INT64), List.of(ColumnCategory.FIELD));
      tableSession.executePreparedInsertNumericRow(prepared, 0, new long[] {42}, new String[0]);
    }
    assertNotSame(buffers.get(0), buffers.get(2));
    assertNotSame(buffers.get(1), buffers.get(3));
  }
}
