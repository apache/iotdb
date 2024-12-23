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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class TabletTest {
  @Test
  public void testSortTablet() {
    Session session = new Session("127.0.0.1", 1234);
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s4", TSDataType.DATE));
    schemaList.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));
    schemaList.add(new MeasurementSchema("s6", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s7", TSDataType.BLOB));
    schemaList.add(new MeasurementSchema("s8", TSDataType.TEXT));
    schemaList.add(new MeasurementSchema("s9", TSDataType.STRING));
    ;
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
    values[0] = new long[] {0, 1, 2};
    values[1] = new long[] {0, 1, 2};
    values[2] = new int[] {0, 1, 2};
    values[3] =
        new LocalDate[] {LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(1), LocalDate.ofEpochDay(2)};
    values[4] = new boolean[] {true, false, true};
    values[5] = new double[] {0.0, 1.0, 2.0};
    values[6] =
        new Binary[] {
          new Binary("0".getBytes(StandardCharsets.UTF_8)),
          new Binary("1".getBytes(StandardCharsets.UTF_8)),
          new Binary("2".getBytes(StandardCharsets.UTF_8))
        };
    values[7] =
        new Binary[] {
          new Binary("0".getBytes(StandardCharsets.UTF_8)),
          new Binary("1".getBytes(StandardCharsets.UTF_8)),
          new Binary("2".getBytes(StandardCharsets.UTF_8))
        };
    values[8] =
        new Binary[] {
          new Binary("0".getBytes(StandardCharsets.UTF_8)),
          new Binary("1".getBytes(StandardCharsets.UTF_8)),
          new Binary("2".getBytes(StandardCharsets.UTF_8))
        };

    tablet.setRowSize(3);

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
    long[] expectedTimestamps = new long[] {0, 1, 2};
    assertArrayEquals(expectedTimestamps, resTimestamps);
    assertArrayEquals(new long[] {1, 2, 0}, ((long[]) tablet.values[0]));
    assertArrayEquals(new long[] {1, 2, 0}, ((long[]) tablet.values[1]));
    assertArrayEquals(new int[] {1, 2, 0}, ((int[]) tablet.values[2]));
    assertArrayEquals(
        new LocalDate[] {LocalDate.ofEpochDay(1), LocalDate.ofEpochDay(2), LocalDate.ofEpochDay(0)},
        ((LocalDate[]) tablet.values[3]));
    assertArrayEquals(new boolean[] {false, true, true}, ((boolean[]) tablet.values[4]));
    assertArrayEquals(new double[] {1.0, 2.0, 0.0}, ((double[]) tablet.values[5]), 0.001);
    assertArrayEquals(
        new Binary[] {
          new Binary("1".getBytes(StandardCharsets.UTF_8)),
          new Binary("2".getBytes(StandardCharsets.UTF_8)),
          new Binary("0".getBytes(StandardCharsets.UTF_8))
        },
        ((Binary[]) tablet.values[6]));
    assertArrayEquals(
        new Binary[] {
          new Binary("1".getBytes(StandardCharsets.UTF_8)),
          new Binary("2".getBytes(StandardCharsets.UTF_8)),
          new Binary("0".getBytes(StandardCharsets.UTF_8))
        },
        ((Binary[]) tablet.values[7]));
    assertArrayEquals(
        new Binary[] {
          new Binary("1".getBytes(StandardCharsets.UTF_8)),
          new Binary("2".getBytes(StandardCharsets.UTF_8)),
          new Binary("0".getBytes(StandardCharsets.UTF_8))
        },
        ((Binary[]) tablet.values[8]));
  }
}
