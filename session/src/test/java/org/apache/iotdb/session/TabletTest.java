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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class TabletTest {
  @Test
  public void testSortTablet() {
    Session session = new Session("127.0.0.1", 1234);
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    // insert three rows data
    Tablet tablet = new Tablet("root.sg1.d1", schemaList, 3);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    /*
    inorder data before inserting
    timestamp   s1
    2           0
    0           1
    1           2
     */
    // inorder timestamps
    timestamps[0] = 2;
    timestamps[1] = 0;
    timestamps[2] = 1;
    // just one column INT64 data
    long[] sensor = (long[]) values[0];
    sensor[0] = 0;
    sensor[1] = 1;
    sensor[2] = 2;
    tablet.rowSize = 3;

    session.sortTablet(tablet);

    /*
    After sorting, if the tablet data is sorted according to the timestamps,
    data in tablet will be
    timestamp   s1
    0           1
    1           2
    2           0

    If the data equal to above tablet, test pass, otherwise test fialed
     */
    long[] resTimestamps = tablet.timestamps;
    long[] resValues = (long[]) tablet.values[0];
    long[] expectedTimestamps = new long[] {0, 1, 2};
    long[] expectedValues = new long[] {1, 2, 0};
    assertArrayEquals(expectedTimestamps, resTimestamps);
    assertArrayEquals(expectedValues, resValues);
  }
}
