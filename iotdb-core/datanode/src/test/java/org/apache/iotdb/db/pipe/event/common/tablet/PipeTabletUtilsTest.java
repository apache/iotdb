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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PipeTabletUtilsTest {

  @Test
  public void testPutValueUnmarksReusedNullRow() {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.FLOAT),
            new MeasurementSchema("s2", TSDataType.FLOAT));
    final Tablet tablet = new Tablet("root.sg.d1", schemas, 2);

    PipeTabletUtils.markNullValue(tablet, 0, 0);
    PipeTabletUtils.markNullValue(tablet, 0, 1);

    PipeTabletUtils.putValue(tablet, 0, 0, TSDataType.FLOAT, 1.0f);
    PipeTabletUtils.putTimestamp(tablet, 0, 1L);
    PipeTabletUtils.compactBitMaps(tablet);

    Assert.assertFalse(tablet.isNull(0, 0));
    Assert.assertTrue(tablet.isNull(0, 1));
  }

  @Test
  public void testPutFieldValueMatchesObjectConversion() {
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
      123456789,
      9876543210L,
      Long.MIN_VALUE,
      -0.0f,
      Double.longBitsToDouble(0x7ff0000000000001L),
      DateUtils.parseDateExpressionToInt(LocalDate.of(2024, 2, 29)),
      new Binary(new byte[] {1, 2, 3}),
      new Binary(new byte[] {4, 5}),
      new Binary(new byte[0]),
      new Binary(new byte[16])
    };
    for (int i = 0; i < types.length; i++) {
      final List<IMeasurementSchema> schemas =
          Collections.singletonList(new MeasurementSchema("s1", types[i]));
      final Tablet expected = new Tablet("root.sg.d1", schemas, 1);
      final Tablet actual = new Tablet("root.sg.d1", schemas, 1);
      final Field field = Type.fromTsDataType(types[i]).getField(values[i]);
      PipeTabletUtils.markNullValue(actual, 0, 0);

      PipeTabletUtils.putValue(expected, 0, 0, types[i], field.getObjectValue(types[i]));
      PipeTabletUtils.putFieldValue(actual, 0, 0, types[i], field);

      Assert.assertFalse(actual.isNull(0, 0));
      Assert.assertEquals(expected.getValue(0, 0), actual.getValue(0, 0));
    }
  }

  @Test
  public void testPutFieldValuePreservesNullFieldDefaults() {
    for (final TSDataType type :
        Arrays.asList(TSDataType.INT32, TSDataType.DATE, TSDataType.STRING, TSDataType.BLOB)) {
      final List<IMeasurementSchema> schemas =
          Collections.singletonList(new MeasurementSchema("s1", type));
      final Tablet expected = new Tablet("root.sg.d1", schemas, 1);
      final Tablet actual = new Tablet("root.sg.d1", schemas, 1);
      PipeTabletUtils.markNullValue(actual, 0, 0);

      PipeTabletUtils.putValue(expected, 0, 0, type, null);
      PipeTabletUtils.putFieldValue(actual, 0, 0, type, new Field(null));

      Assert.assertFalse(actual.isNull(0, 0));
      Assert.assertEquals(expected.getValue(0, 0), actual.getValue(0, 0));
    }
  }
}
