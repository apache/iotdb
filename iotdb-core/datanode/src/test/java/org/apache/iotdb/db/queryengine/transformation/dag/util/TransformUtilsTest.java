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

package org.apache.iotdb.db.queryengine.transformation.dag.util;

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.ValueRecorder;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TransformUtilsTest {

  @Test
  public void testSplitWindowForSupportedTypes() {
    assertStateWindowSplit(TSDataType.INT32, 1, 2, 3);
    assertStateWindowSplit(TSDataType.INT64, 1L, 2L, 3L);
    assertStateWindowSplit(TSDataType.FLOAT, 1F, 2F, 3F);
    assertStateWindowSplit(TSDataType.DOUBLE, 1D, 2D, 3D);
    assertStateWindowSplit(TSDataType.BOOLEAN, false, false, true);
    assertStateWindowSplit(
        TSDataType.TEXT,
        new Binary("a", TSFileConfig.STRING_CHARSET),
        new Binary("a", TSFileConfig.STRING_CHARSET),
        new Binary("b", TSFileConfig.STRING_CHARSET));
  }

  @Test
  public void testSplitWindowForUnsupportedType() {
    final Column values = createColumn(TSDataType.DATE, 20260717, 20260718);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            TransformUtils.splitWindowForStateWindow(
                TSDataType.DATE, new ValueRecorder(), 1, values, 1));
  }

  @Test
  public void testTransformationTypeUtils() throws Exception {
    assertEquals(
        1D, TypeUtils.castValueToDouble(createColumn(TSDataType.INT32, 1), TSDataType.INT32, 0), 0);
    assertEquals(
        2D, TypeUtils.castValueToDouble(createColumn(TSDataType.DATE, 2), TSDataType.DATE, 0), 0);
    assertEquals(
        3D,
        TypeUtils.castValueToDouble(createColumn(TSDataType.INT64, 3L), TSDataType.INT64, 0),
        0);
    assertEquals(
        4D,
        TypeUtils.castValueToDouble(
            createColumn(TSDataType.TIMESTAMP, 4L), TSDataType.TIMESTAMP, 0),
        0);
    assertEquals(
        5.5D,
        TypeUtils.castValueToDouble(createColumn(TSDataType.FLOAT, 5.5F), TSDataType.FLOAT, 0),
        0);
    assertEquals(
        6.5D,
        TypeUtils.castValueToDouble(createColumn(TSDataType.DOUBLE, 6.5D), TSDataType.DOUBLE, 0),
        0);
    assertEquals(
        1D,
        TypeUtils.castValueToDouble(createColumn(TSDataType.BOOLEAN, true), TSDataType.BOOLEAN, 0),
        0);

    final Column textColumn =
        createColumn(TSDataType.TEXT, new Binary("text", TSFileConfig.STRING_CHARSET));
    assertThrows(
        QueryProcessException.class,
        () -> TypeUtils.castValueToDouble(textColumn, TSDataType.TEXT, 0));
  }

  private static void assertStateWindowSplit(
      TSDataType dataType, Object first, Object second, Object third) {
    final Column values = createColumn(dataType, first, second, third);
    final ValueRecorder valueRecorder = new ValueRecorder();

    assertFalse(TransformUtils.splitWindowForStateWindow(dataType, valueRecorder, 1, values, 1));
    assertTrue(TransformUtils.splitWindowForStateWindow(dataType, valueRecorder, 1, values, 2));
  }

  private static Column createColumn(TSDataType dataType, Object... values) {
    final Type type = Type.fromTsDataType(dataType);
    final ColumnBuilder builder = TypeUtils.initColumnBuilder(dataType, values.length);
    for (final Object value : values) {
      type.writeObject(builder, value);
    }
    return builder.build();
  }
}
