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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
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
  public void testPutObjectValue() {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(new MeasurementSchema("s1", TSDataType.OBJECT));
    final Tablet tablet = new Tablet("root.sg.d1", schemas, 1);
    final Binary value = new Binary(new byte[] {1, 2, 3});

    PipeTabletUtils.markNullValue(tablet, 0, 0);
    PipeTabletUtils.putValue(tablet, 0, 0, TSDataType.OBJECT, value);

    Assert.assertFalse(tablet.isNull(0, 0));
    Assert.assertArrayEquals(value.getValues(), ((Binary[]) tablet.getValues()[0])[0].getValues());
  }
}
