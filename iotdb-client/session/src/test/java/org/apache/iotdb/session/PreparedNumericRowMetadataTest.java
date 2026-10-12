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
import org.apache.iotdb.service.rpc.thrift.TSPrepareNumericRowReq;
import org.apache.iotdb.session.i18n.SessionMessages;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PreparedNumericRowMetadataTest {
  private static final String INVALID_ARGUMENTS =
      SessionMessages
          .EXCEPTION_NUMERIC_ROW_ARGUMENTS_MUST_BE_NON_NULL_COLUMN_METADATA_LENGTHS_AND_PER_CATEGORY_VALUE_COUNTS_MUST_MATCH_EB12B233;
  private static final String INVALID_CATEGORIES =
      SessionMessages
          .EXCEPTION_NUMERIC_ROWS_SUPPORT_NUMERIC_FIELD_COLUMNS_AND_STRING_TAG_COLUMNS_ONLY_87159FFA;

  @Test
  public void testNullMetadataRejectedBeforeRpc() throws Exception {
    assertInvalid(
        null,
        List.of("v"),
        List.of(TSDataType.DOUBLE),
        List.of(ColumnCategory.FIELD),
        INVALID_ARGUMENTS);
    assertInvalid(
        "t", null, List.of(TSDataType.DOUBLE), List.of(ColumnCategory.FIELD), INVALID_ARGUMENTS);
    assertInvalid("t", List.of("v"), null, List.of(ColumnCategory.FIELD), INVALID_ARGUMENTS);
    assertInvalid("t", List.of("v"), List.of(TSDataType.DOUBLE), null, INVALID_ARGUMENTS);
  }

  @Test
  public void testNullMetadataElementsRejectedBeforeRpc() throws Exception {
    assertInvalid(
        "t",
        Arrays.asList("tag", null),
        List.of(TSDataType.STRING, TSDataType.DOUBLE),
        List.of(ColumnCategory.TAG, ColumnCategory.FIELD),
        INVALID_ARGUMENTS);
    assertInvalid(
        "t",
        List.of("tag", "v"),
        Arrays.asList(TSDataType.STRING, null),
        List.of(ColumnCategory.TAG, ColumnCategory.FIELD),
        INVALID_ARGUMENTS);
    assertInvalid(
        "t",
        List.of("tag", "v"),
        List.of(TSDataType.STRING, TSDataType.DOUBLE),
        Arrays.asList(ColumnCategory.TAG, null),
        INVALID_ARGUMENTS);
  }

  @Test
  public void testMetadataLengthMismatchRejectedBeforeRpc() throws Exception {
    final List<String> names = List.of("a", "b");
    final List<TSDataType> types = List.of(TSDataType.DOUBLE, TSDataType.DOUBLE);
    final List<ColumnCategory> categories = List.of(ColumnCategory.FIELD, ColumnCategory.FIELD);
    assertInvalid("t", names.subList(0, 1), types, categories, INVALID_ARGUMENTS);
    assertInvalid("t", names, types.subList(0, 1), categories, INVALID_ARGUMENTS);
    assertInvalid("t", names, types, categories.subList(0, 1), INVALID_ARGUMENTS);
    assertInvalid(
        "t",
        names,
        List.of(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
        categories,
        INVALID_ARGUMENTS);
    assertInvalid(
        "t",
        names,
        types,
        List.of(ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD),
        INVALID_ARGUMENTS);
  }

  @Test
  public void testAttributeColumnRejectedBeforeRpc() throws Exception {
    assertInvalid(
        "t",
        List.of("tag", "attr", "v"),
        List.of(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE),
        List.of(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD),
        INVALID_CATEGORIES);
  }

  @Test
  public void testNonStringTagRejectedBeforeRpc() throws Exception {
    for (TSDataType type : List.of(TSDataType.INT64, TSDataType.TEXT)) {
      assertInvalid(
          "t",
          List.of("tag", "v"),
          List.of(type, TSDataType.DOUBLE),
          List.of(ColumnCategory.TAG, ColumnCategory.FIELD),
          INVALID_CATEGORIES);
    }
  }

  @Test
  public void testMixedColumnOrderPreservesMetadataAndValues() throws Exception {
    final Session session = mock(Session.class);
    final SessionConnection connection = mock(SessionConnection.class);
    when(session.getDefaultSessionConnection()).thenReturn(connection);
    when(session.getDatabase()).thenReturn("db");
    when(connection.getSessionId()).thenReturn(10L);
    when(connection.prepareNumericRow(any())).thenReturn(42L);
    final TableSession tableSession = new TableSession(session);
    final List<String> names = List.of("v1", "tag1", "v2", "tag2");
    final List<TSDataType> types =
        List.of(TSDataType.DOUBLE, TSDataType.STRING, TSDataType.INT32, TSDataType.STRING);
    final List<ColumnCategory> categories =
        List.of(ColumnCategory.FIELD, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.TAG);
    final NumericRowPreparedStatement prepared =
        tableSession.prepareInsertNumericRow("t", names, types, categories);
    final ArgumentCaptor<TSPrepareNumericRowReq> request =
        ArgumentCaptor.forClass(TSPrepareNumericRowReq.class);
    verify(connection).prepareNumericRow(request.capture());
    assertMetadata(prepared, request.getValue());
    doAnswer(
            invocation -> {
              final ByteBuffer values = invocation.getArgument(2);
              final ByteBuffer tags = invocation.getArgument(3);
              assertEquals("a", ReadWriteIOUtils.readString(tags));
              assertEquals("b", ReadWriteIOUtils.readString(tags));
              assertFalse(tags.hasRemaining());
              assertEquals(12.5, values.getDouble(), 0.0);
              assertEquals(7, values.getInt());
              assertFalse(values.hasRemaining());
              return null;
            })
        .when(connection)
        .executePreparedNumericRow(eq(42L), eq(123L), any(ByteBuffer.class), any(ByteBuffer.class));
    tableSession.executePreparedInsertNumericRow(
        prepared, 123L, new long[] {Double.doubleToRawLongBits(12.5), 7}, new String[] {"a", "b"});
    verify(connection)
        .executePreparedNumericRow(eq(42L), eq(123L), any(ByteBuffer.class), any(ByteBuffer.class));
    assertEquals(List.of("v1", "tag1", "v2", "tag2"), names);
    assertEquals(
        List.of(TSDataType.DOUBLE, TSDataType.STRING, TSDataType.INT32, TSDataType.STRING), types);
    assertEquals(
        List.of(ColumnCategory.FIELD, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.TAG),
        categories);
  }

  private static void assertMetadata(
      NumericRowPreparedStatement prepared, TSPrepareNumericRowReq request) {
    assertEquals("t", prepared.getTableName());
    assertEquals("db", prepared.getDatabase());
    assertEquals(2, prepared.getTagCount());
    assertEquals(List.of("tag1", "tag2", "v1", "v2"), prepared.getColumnNames());
    assertEquals(
        List.of(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE, TSDataType.INT32),
        prepared.getDataTypes());
    assertEquals(
        List.of(ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
        prepared.getColumnCategories());
    assertEquals("t", request.getTableName());
    assertEquals(prepared.getColumnNames(), request.getMeasurements());
    assertEquals(
        prepared.getDataTypes().stream().map(TSDataType::ordinal).toList(), request.getDataTypes());
    assertEquals(
        prepared.getColumnCategories().stream().map(c -> (byte) c.ordinal()).toList(),
        request.getColumnCategories());
  }

  private static void assertInvalid(
      String table,
      List<String> names,
      List<TSDataType> types,
      List<ColumnCategory> categories,
      String message)
      throws Exception {
    final Session session = mock(Session.class);
    try {
      new TableSession(session).prepareInsertNumericRow(table, names, types, categories);
      fail("Invalid metadata must be rejected before preparing a statement");
    } catch (IllegalArgumentException e) {
      assertEquals(message, e.getMessage());
    }
    verifyNoMoreInteractions(session);
  }
}
