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

package org.apache.iotdb.commons.udf.utils;

import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.RowImpl;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MasterRepairUtilTest {

  @Test
  public void testNumericValues() throws Exception {
    // Use real boxed values to catch casts to Double and exercise nonzero column indices.
    RowImpl row =
        new RowImpl(
            new TSDataType[] {
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE,
              TSDataType.TIMESTAMP
            });
    row.setRowRecord(new Object[] {-42, Long.MAX_VALUE, 0.1f, -1.25d, 123L});

    assertEquals(-42d, MasterRepairUtil.getValueAsDouble(row, 0), 0d);
    assertEquals((double) Long.MAX_VALUE, MasterRepairUtil.getValueAsDouble(row, 1), 0d);
    assertEquals((double) 0.1f, MasterRepairUtil.getValueAsDouble(row, 2), 0d);
    assertEquals(-1.25d, MasterRepairUtil.getValueAsDouble(row, 3), 0d);
  }

  @Test
  public void testRejectNonNumericTypes() throws Exception {
    // In particular, DATE and TIMESTAMP must not become valid repair measurements.
    for (Type type :
        EnumSet.complementOf(EnumSet.of(Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE))) {
      Row row = mock(Row.class);
      when(row.getDataType(1)).thenReturn(type);

      Exception exception =
          assertThrows(Exception.class, () -> MasterRepairUtil.getValueAsDouble(row, 1));

      assertEquals(CommonMessages.VALUE_NOT_NUMERIC, exception.getMessage());
      verify(row, never()).getDouble(1);
    }
  }

  @Test
  public void testWrapIOException() throws Exception {
    // A failed value read must retain the original cause and row timestamp in the message.
    Row row = mock(Row.class);
    IOException cause = new IOException("test read failure");
    when(row.getDataType(1)).thenReturn(Type.INT32);
    when(row.getDouble(1)).thenThrow(cause);
    when(row.getTime()).thenReturn(123L);

    Exception exception =
        assertThrows(Exception.class, () -> MasterRepairUtil.getValueAsDouble(row, 1));

    assertSame(cause, exception.getCause());
    assertEquals(CommonMessages.FAIL_TO_GET_DATA_TYPE_IN_ROW + 123L, exception.getMessage());
  }
}
