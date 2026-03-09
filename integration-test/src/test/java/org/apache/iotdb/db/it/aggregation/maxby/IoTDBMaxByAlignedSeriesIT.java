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

package org.apache.iotdb.db.it.aggregation.maxby;

import org.apache.iotdb.it.env.EnvFactory;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

public class IoTDBMaxByAlignedSeriesIT extends IoTDBMaxByIT {
  protected static final String[] ALIGNED_DATASET =
      new String[] {
        // x input
        "CREATE ALIGNED TIMESERIES root.db.d1(x1 INT32, x2 INT64, x3 FLOAT, x4 DOUBLE, x5 BOOLEAN, x6 TEXT, x7 STRING, x8 BLOB, x9 DATE, x10 TIMESTAMP)",
        // y input
        "CREATE ALIGNED TIMESERIES root.db.d1(y1 INT32, y2 INT64, y3 FLOAT, y4 DOUBLE, y5 BOOLEAN, y6 TEXT, y7 STRING, y8 BLOB, y9 DATE, y10 TIMESTAMP)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) values(1, 1, 1, 1, 1, true, \"1\", '1', X'11', '2024-01-01', 1)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) values(2, 2, 2, 2, 2, false, \"2\", '2', X'22', '2024-01-02', 2)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) values(3, 3, 3, 3, 3, false, \"3\", '3', X'33', '2024-01-03', 3)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(2, 2, 2, 2, 2, true, \"4\", '2', X'22', '2024-01-02', 2)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(3, 3, 3, 3, 3, false, \"3\", '3', X'33', '2024-01-03', 3)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(4, 4, 4, 4, 4, false, \"4\", '4', X'44', '2024-01-04', 4)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,y7,y8,y9,y10) values(8, 3, 3, 3, 3, false, \"3\", '3', X'33', '2024-01-03', 3)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(8, 8, 8, 8, 8, false, \"4\", '8', X'44', '2024-01-08', 8)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,y7,y8,y9,y10) values(12, 3, 3, 3, 3, false, \"3\", '3', X'33', '2024-01-03', 3)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(12, 9, 9, 9, 9, false, \"4\", '9', X'44', '2024-01-09', 9)",
        "INSERT INTO root.db.d1(timestamp,x1,x2,x3,x4,x5,x6,y7,y8,y9,y10) values(13, 4, 4, 4, 4, false, \"4\",'4', X'44', '2024-01-04', 4)",
        "INSERT INTO root.db.d1(timestamp,y1,y2,y3,y4,y5,y6,y7,y8,y9,y10) values(13, 9, 9, 9, 9, false, \"4\",'9', X'44', '2024-01-09', 9)",
        "flush",
        // For Align By Device
        "CREATE ALIGNED TIMESERIES root.db.d2(x1 INT32, x2 INT64, x3 FLOAT, x4 DOUBLE, x5 BOOLEAN, x6 TEXT)",
        "CREATE ALIGNED TIMESERIES root.db.d2(y1 INT32, y2 INT64, y3 FLOAT, y4 DOUBLE, y5 BOOLEAN, y6 TEXT)",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(1, 1, 1, 1, 1, true, \"1\")",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(2, 2, 2, 2, 2, false, \"2\")",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(3, 3, 3, 3, 3, false, \"3\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(2, 2, 2, 2, 2, true, \"4\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(3, 3, 3, 3, 3, false, \"3\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(4, 4, 4, 4, 4, false, \"4\")",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(8, 3, 3, 3, 3, false, \"3\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(8, 8, 8, 8, 8, false, \"4\")",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(12, 3, 3, 3, 3, false, \"3\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(12, 8, 8, 8, 8, false, \"4\")",
        "INSERT INTO root.db.d2(timestamp,x1,x2,x3,x4,x5,x6) values(13, 4, 4, 4, 4, false, \"4\")",
        "INSERT INTO root.db.d2(timestamp,y1,y2,y3,y4,y5,y6) values(13, 8, 8, 8, 8, false, \"4\")",
        "insert into root.sg.d1(time,s1,s2) values(1,1,1);",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(ALIGNED_DATASET);
  }

  @Test
  public void maxMinByTimeTest() {
    String[] expectedHeader =
        new String[] {"Device", "max_by(Time, s1 + 1)", "min_by(Time, s1 + 1)"};
    String[] retArray = new String[] {"root.sg.d1,1,1,"};
    resultSetEqualTest(
        "select max_by(time, s1+1), min_by(time, s1+1)  from root.sg.*  align by device",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Device", "max_by(Time, s1)", "min_by(Time, s1)"};
    resultSetEqualTest(
        "select max_by(time, s1), min_by(time, s1) from root.sg.* where s1>0  align by device",
        expectedHeader,
        retArray);
  }
}
