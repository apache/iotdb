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

package org.apache.iotdb.db.pipe.event.common.row;

import org.apache.iotdb.pipe.api.type.Binary;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BytesUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PipeRowTest {

  @Test
  public void testGetObjectForSupportedTypes() {
    final LocalDate date = LocalDate.of(2026, 8, 12);
    final PipeRow row =
        new PipeRow(
            0,
            "root.test.device",
            false,
            null,
            new long[] {1L},
            new TSDataType[] {
              TSDataType.BOOLEAN,
              TSDataType.INT32,
              TSDataType.DATE,
              TSDataType.INT64,
              TSDataType.TIMESTAMP,
              TSDataType.FLOAT,
              TSDataType.DOUBLE,
              TSDataType.TEXT,
              TSDataType.BLOB,
              TSDataType.STRING
            },
            new Object[] {
              new boolean[] {true},
              new int[] {2},
              new LocalDate[] {date},
              new long[] {3L},
              new long[] {4L},
              new float[] {5.5F},
              new double[] {6.5D},
              new org.apache.tsfile.utils.Binary[] {BytesUtils.valueOf("text")},
              new org.apache.tsfile.utils.Binary[] {BytesUtils.valueOf("blob")},
              new org.apache.tsfile.utils.Binary[] {BytesUtils.valueOf("string")}
            },
            null,
            new String[] {
              "boolean",
              "int32",
              "date",
              "int64",
              "timestamp",
              "float",
              "double",
              "text",
              "blob",
              "string"
            });

    assertEquals(Boolean.TRUE, row.getObject(0));
    assertEquals(2, row.getObject(1));
    assertEquals(date, row.getObject(2));
    assertEquals(3L, row.getObject(3));
    assertEquals(4L, row.getObject(4));
    assertEquals(5.5F, row.getObject(5));
    assertEquals(6.5D, row.getObject(6));
    assertBinaryEquals("text", row.getObject(7));
    assertBinaryEquals("blob", row.getObject(8));
    assertBinaryEquals("string", row.getObject(9));
  }

  @Test
  public void testGetObjectForUnsupportedType() {
    final PipeRow row =
        new PipeRow(
            0,
            "root.test.device",
            false,
            null,
            new long[] {1L},
            new TSDataType[] {TSDataType.OBJECT},
            new Object[] {new Object[] {new Object()}},
            null,
            new String[] {"object"});

    Assert.assertThrows(IllegalArgumentException.class, () -> row.getObject(0));
  }

  private static void assertBinaryEquals(final String expected, final Object actual) {
    assertArrayEquals(Binary.stringToBytes(expected), ((Binary) actual).getValues());
  }
}
