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

package org.apache.iotdb.relational.it.query.old.aligned;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.relational.it.query.old.aligned.TableUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBPredicatePushDownTableIT {

  private final String database = "db";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setMaxTsBlockLineNumber(3);
    EnvFactory.getEnv().initClusterEnvironment();
    TableUtils.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAlignedRawDataAlignByTime1() {
    String[] expectedHeader1 = new String[] {"Time", "s2", "s3"};
    String[] retArray1 =
        new String[] {
          "10,10,10,",
          "11,11,11,",
          "12,12,12,",
          "14,14,14,",
          "15,15,15,",
          "16,16,16,",
          "17,17,17,",
          "18,18,18,",
          "19,19,19,",
          "20,20,20,"
        };
    tableResultSetEqualTest(
        "select Time,s2, s3 from table0 where device='d1' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader1,
        retArray1,
        database);

    String[] expectedHeader2 = new String[] {"Time", "s3"};
    String[] retArray2 =
        new String[] {
          "10,10,", "11,11,", "12,12,", "14,14,", "15,15,", "16,16,", "17,17,", "18,18,", "19,19,",
          "20,20,"
        };
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d1' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader2,
        retArray2,
        database);

    String[] expectedHeader3 = new String[] {"Time", "s2"};
    String[] retArray3 =
        new String[] {
          "10,10,", "11,11,", "12,12,", "14,14,", "15,15,", "16,16,", "17,17,", "18,18,", "19,19,",
          "20,20,"
        };
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d1' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader3,
        retArray3,
        database);

    String[] expectedHeader4 = new String[] {"Time", "s2"};
    String[] retArray4 = new String[] {"14,14,", "15,15,"};
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d1' and s2 - 1 >= 9 and s2 < 30 offset 3 limit 2",
        expectedHeader4,
        retArray4,
        database);
  }

  // TODO fix it
  @Test
  public void testAlignedRawDataAlignByTime2() {
    String[] expectedHeader1 = new String[] {"Time", "s2", "_col2"};
    String[] retArray1 =
        new String[] {
          "3,null,30001.0,",
          "13,130000,130001.0,",
          "16,16,17.0,",
          "17,17,18.0,",
          "18,18,19.0,",
          "19,19,20.0,",
          "20,20,21.0,",
          "21,null,22.0,",
          "22,null,23.0,",
          "23,null,230001.0,",
          "24,null,25.0,",
          "25,null,26.0,",
          "26,null,27.0,",
          "27,null,28.0,",
          "28,null,29.0,",
          "29,null,30.0,",
          "30,null,31.0,",
        };
    tableResultSetEqualTest(
        "select Time,s2, s3 + 1 from table0 where device='d1' and s3 + 1 > 16",
        expectedHeader1,
        retArray1,
        database);

    String[] expectedHeader2 = new String[] {"Time", "s2"};
    String[] retArray2 =
        new String[] {
          "3,null,",
          "13,130000,",
          "16,16,",
          "17,17,",
          "18,18,",
          "19,19,",
          "20,20,",
          "21,null,",
          "22,null,",
          "23,null,",
          "24,null,",
          "25,null,",
          "26,null,",
          "27,null,",
          "28,null,",
          "29,null,",
          "30,null,",
        };
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d1' and s3 + 1 > 16",
        expectedHeader2,
        retArray2,
        database);

    String[] expectedHeader3 = new String[] {"Time", "s3"};
    String[] retArray3 =
        new String[] {
          "3,30000,",
          "13,130000,",
          "16,16,",
          "17,17,",
          "18,18,",
          "19,19,",
          "20,20,",
          "21,21,",
          "22,22,",
          "23,230000,",
          "24,24,",
          "25,25,",
          "26,26,",
          "27,27,",
          "28,28,",
          "29,29,",
          "30,30,",
        };
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d1' and s3 + 1 > 16",
        expectedHeader3,
        retArray3,
        database);

    String[] expectedHeader4 = new String[] {"Time", "s3"};
    String[] retArray4 = new String[] {"3,30000,", "13,130000,", "16,16,"};
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d1' and s3 + 1 > 16 limit 3",
        expectedHeader4,
        retArray4,
        database);
  }

  @Test
  public void testNonAlignedRawDataAlignByTime1() {
    String[] expectedHeader1 = new String[] {"Time", "s2", "s3"};
    String[] retArray1 =
        new String[] {
          "10,10,10,",
          "11,11,11,",
          "12,12,12,",
          "13,13,13,",
          "14,14,14,",
          "15,15,15,",
          "16,16,16,",
          "17,17,17,",
          "18,18,18,",
          "19,19,19,",
          "20,20,20,"
        };
    tableResultSetEqualTest(
        "select Time,s2, s3 from table0 where device='d2' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader1,
        retArray1,
        database);

    String[] expectedHeader2 = new String[] {"Time", "s3"};
    String[] retArray2 =
        new String[] {
          "10,10,", "11,11,", "12,12,", "13,13,", "14,14,", "15,15,", "16,16,", "17,17,", "18,18,",
          "19,19,", "20,20,"
        };
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d2' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader2,
        retArray2,
        database);

    String[] expectedHeader3 = new String[] {"Time", "s2"};
    String[] retArray3 =
        new String[] {
          "10,10,", "11,11,", "12,12,", "13,13,", "14,14,", "15,15,", "16,16,", "17,17,", "18,18,",
          "19,19,", "20,20,"
        };
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d2' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader3,
        retArray3,
        database);

    String[] expectedHeader4 = new String[] {"Time", "s2"};
    String[] retArray4 = new String[] {"12,12,", "13,13,", "14,14,"};
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d2' and s2 - 1 >= 9 and s2 < 30 offset 2 limit 3",
        expectedHeader4,
        retArray4,
        database);
  }

  @Test
  public void testNonAlignedRawDataAlignByTime2() {
    String[] expectedHeader1 = new String[] {"Time", "s2", "s3"};
    String[] retArray1 =
        new String[] {
          "16,16,16,",
          "17,17,17,",
          "18,18,18,",
          "19,19,19,",
          "20,20,20,",
          "21,null,21,",
          "22,null,22,",
          "23,null,23,",
          "24,null,24,",
          "25,null,25,",
          "26,null,26,",
          "27,null,27,",
          "28,null,28,",
          "29,null,29,",
          "30,null,30,",
        };
    tableResultSetEqualTest(
        "select Time,s2, s3 from table0 where device='d2' and s3 + 1 > 16",
        expectedHeader1,
        retArray1,
        database);

    String[] expectedHeader2 = new String[] {"Time", "s2"};
    String[] retArray2 =
        new String[] {
          "16,16,",
          "17,17,",
          "18,18,",
          "19,19,",
          "20,20,",
          "21,null,",
          "22,null,",
          "23,null,",
          "24,null,",
          "25,null,",
          "26,null,",
          "27,null,",
          "28,null,",
          "29,null,",
          "30,null,",
        };
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d2' and s3 + 1 > 16",
        expectedHeader2,
        retArray2,
        database);

    String[] expectedHeader3 = new String[] {"Time", "s3"};
    String[] retArray3 =
        new String[] {
          "16,16,", "17,17,", "18,18,", "19,19,", "20,20,", "21,21,", "22,22,", "23,23,", "24,24,",
          "25,25,", "26,26,", "27,27,", "28,28,", "29,29,", "30,30,",
        };
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d2' and s3 + 1 > 16",
        expectedHeader3,
        retArray3,
        database);

    String[] expectedHeader4 = new String[] {"Time", "s3"};
    String[] retArray4 =
        new String[] {
          "26,26,", "27,27,", "28,28,", "29,29,", "30,30,",
        };
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d2' and s3 + 1 > 16 offset 10",
        expectedHeader4,
        retArray4,
        database);
  }

  @Ignore
  @Test
  public void testAlignedAggregationAlignByTime1() {
    String[] expectedHeader1 = new String[] {"count(d1.s2),count(d1.s3),"};
    String[] retArray1 =
        new String[] {
          "10,10,",
        };
    tableResultSetEqualTest(
        "select count(s2), count(s3) from d1 where s2  - 1 >= 9 and s2 < 30",
        expectedHeader1,
        retArray1,
        database);

    String[] expectedHeader2 = new String[] {"count(d1.s3),"};
    String[] retArray2 = new String[] {"10,"};
    tableResultSetEqualTest(
        "select count(s3) from d1 where s2  - 1 >= 9 and s2 < 30",
        expectedHeader2,
        retArray2,
        database);

    String[] expectedHeader3 = new String[] {"count(d1.s2),"};
    String[] retArray3 =
        new String[] {
          "10,",
        };
    tableResultSetEqualTest(
        "select count(s2) from d1 where s2  - 1 >= 9 and s2 < 30",
        expectedHeader3,
        retArray3,
        database);
  }

  //  @Ignore
  //  @Test
  //  public void testAlignedAggregationAlignByTime2() {
  //    String[] expectedHeader1 = new String[]{"count(d1.s2),count(d1.s3 + 1),"};
  //    String[] retArray1 =
  //        new String[] {
  //          "6,17,",
  //        };
  //    tableResultSetEqualTest(
  //        "select count(s2), count(s3 + 1) from d1 where s3  + 1 > 16",
  //        expectedHeader1,
  //        retArray1, database);
  //
  //    String[] expectedHeader2 = "count(d1.s2),";
  //    String[] retArray2 =
  //        new String[] {
  //          "6,",
  //        };
  //    tableResultSetEqualTest(
  //        "select count(s2) from d1 where s3  + 1 > 16", expectedHeader2, retArray2, database);
  //
  //    String[] expectedHeader3 = "count(d1.s3),";
  //    String[] retArray3 =
  //        new String[] {
  //          "17,",
  //        };
  //    tableResultSetEqualTest(
  //        "select count(s3) from d1 where s3  + 1 > 16", expectedHeader3, retArray3, database);
  //  }
  //
  //  @Ignore
  //  @Test
  //  public void testNonAlignedAggregationAlignByTime1() {
  //    String[] expectedHeader1 = "count(d2.s2),count(d2.s3),";
  //    String[] retArray1 = new String[] {"11,11,"};
  //    tableResultSetEqualTest(
  //        "select count(s2), count(s3) from d2 where s2  - 1 >= 9 and s2 < 30",
  //        expectedHeader1,
  //        retArray1, database);
  //
  //    String[] expectedHeader2 = "count(d2.s3),";
  //    String[] retArray2 = new String[] {"11,"};
  //    tableResultSetEqualTest(
  //        "select count(s3) from d2 where s2  - 1 >= 9 and s2 < 30",
  //        expectedHeader2,
  //        retArray2, database);
  //
  //    String[] expectedHeader3 = "count(d2.s2),";
  //    String[] retArray3 = new String[] {"11,"};
  //    tableResultSetEqualTest(
  //        "select count(s2) from d2 where s2  - 1 >= 9 and s2 < 30",
  //        expectedHeader3,
  //        retArray3, database);
  //  }
  //
  //  @Ignore
  //  @Test
  //  public void testNonAlignedAggregationAlignByTime2() {
  //    String[] expectedHeader1 = "count(d2.s2),count(d2.s3),";
  //    String[] retArray1 =
  //        new String[] {
  //          "5,15,",
  //        };
  //    tableResultSetEqualTest(
  //        "select count(s2), count(s3) from d2 where s3  + 1 > 16",
  //        expectedHeader1,
  //        retArray1, database);
  //
  //    String[] expectedHeader2 = "count(d2.s2),";
  //    String[] retArray2 =
  //        new String[] {
  //          "5,",
  //        };
  //    tableResultSetEqualTest(
  //        "select count(s2) from d2 where s3  + 1 > 16", expectedHeader2, retArray2, database);
  //
  //    String[] expectedHeader3 = "count(d2.s3),";
  //    String[] retArray3 =
  //        new String[] {
  //          "15,",
  //        };
  //    tableResultSetEqualTest(
  //        "select count(s3) from d2 where s3  + 1 > 16", expectedHeader3, retArray3, database);
  //  }
  //
  //  @Ignore
  //  @Test
  //  public void testMixAggregationAlignByTime() {
  //    String[] expectedHeader1 =
  //        "count(d1.s2),count(d2.s2),count(d1.s3),count(d2.s3),";
  //    String[] retArray1 =
  //        new String[] {
  //          "10,10,10,10,",
  //        };
  //    tableResultSetEqualTest(
  //        "select count(s2), count(s3) from d1, d2 where s2  - 1 >= 9 and s2 < 30",
  //        expectedHeader1,
  //        retArray1, database);
  //  }
  //
  //  @Ignore
  //  @Test
  //  public void testAlignedGroupByTimeAlignByTime1() {
  //    String[] expectedHeader = "Time,count(d1.s2),sum(d1.s3),";
  //    String[] retArray = new String[] {"1,1,10.0,", "11,9,142.0,", "21,0,null,", "31,0,null,"};
  //    tableResultSetEqualTest(
  //        "select count(s2), sum(s3) from d1 where s2  - 1 >= 9 and s2 < 30 group by ([1, 41),
  // 10ms)",
  //        expectedHeader,
  //        retArray, database);
  //  }
  //
  //  @Ignore
  //  @Test
  //  public void testAlignedGroupByTimeAlignByTime2() {
  //    String[] expectedHeader = "Time,count(d1.s2),sum(d1.s3),";
  //    String[] retArray =
  //        new String[] {"1,0,30000.0,", "11,6,130090.0,", "21,0,230232.0,", "31,0,null,"};
  //    tableResultSetEqualTest(
  //        "select count(s2), sum(s3) from d1 where s3  + 1 > 16 group by ([1, 41), 10ms)",
  //        expectedHeader,
  //        retArray, database);
  //  }
  //
  //  @Ignore
  //  @Test
  //  public void testNonAlignedGroupByTimeAlignByTime1() {
  //    String[] expectedHeader = "Time,count(d2.s2),sum(d2.s3 + 1),";
  //    String[] retArray = new String[] {"1,1,11.0,", "11,10,165.0,", "21,0,null,", "31,0,null,"};
  //    tableResultSetEqualTest(
  //        "select count(s2), sum(s3 + 1) from d2 where s2  - 1 >= 9 and s2 < 30 group by ([1, 41),
  // 10ms)",
  //        expectedHeader,
  //        retArray, database);
  //  }
  //
  //  @Ignore
  //  @Test
  //  public void testNonAlignedGroupByTimeAlignByTime2() {
  //    String[] expectedHeader = "Time,count(d2.s2),sum(d2.s3),";
  //    String[] retArray = new String[] {"1,0,null,", "11,5,90.0,", "21,0,255.0,", "31,0,null,"};
  //    tableResultSetEqualTest(
  //        "select count(s2), sum(s3) from d2 where s3 + 1 > 16 group by ([1, 41), 10ms)",
  //        expectedHeader,
  //        retArray, database);
  //  }
  //
  //  @Ignore
  //  @Test
  //  public void testMixGroupByTimeAlignByTime() {
  //    String[] expectedHeader =
  //        "Time,count(d1.s2),count(d2.s2),sum(d1.s3),sum(d2.s3),";
  //    String[] retArray =
  //        new String[] {
  //          "1,1,1,10.0,10.0,", "11,9,9,142.0,142.0,", "21,0,0,null,null,", "31,0,0,null,null,"
  //        };
  //    tableResultSetEqualTest(
  //        "select count(s2), sum(s3) from d1, d2 where s2 - 1 >= 9 and s2 < 30 group by ([1, 41),
  // 10ms)",
  //        expectedHeader,
  //        retArray, database);
  //  }

  @Test
  public void testRawDataAlignByDevice1() {
    String[] expectedHeader = new String[] {"Time", "Device", "s2", "s3"};
    String[] retArray =
        new String[] {
          "10,d2,10,10,",
          "11,d2,11,11,",
          "12,d2,12,12,",
          "13,d2,13,13,",
          "14,d2,14,14,",
          "15,d2,15,15,",
          "16,d2,16,16,",
          "17,d2,17,17,",
          "18,d2,18,18,",
          "19,d2,19,19,",
          "20,d2,20,20,",
          "10,d1,10,10,",
          "11,d1,11,11,",
          "12,d1,12,12,",
          "14,d1,14,14,",
          "15,d1,15,15,",
          "16,d1,16,16,",
          "17,d1,17,17,",
          "18,d1,18,18,",
          "19,d1,19,19,",
          "20,d1,20,20,"
        };
    tableResultSetEqualTest(
        "select Time, Device,s2, s3 from table0 where s2 - 1 >= 9 and s2 < 30 order by device desc",
        expectedHeader,
        retArray,
        database);
  }

  @Test
  public void testRawDataAlignByDevice2() {
    String[] expectedHeader = new String[] {"Time", "Device", "s2", "_col3"};
    String[] retArray =
        new String[] {
          "3,d1,null,30001.0,",
          "13,d1,130000,130001.0,",
          "16,d1,16,17.0,",
          "17,d1,17,18.0,",
          "18,d1,18,19.0,",
          "19,d1,19,20.0,",
          "20,d1,20,21.0,",
          "21,d1,null,22.0,",
          "22,d1,null,23.0,",
          "23,d1,null,230001.0,",
          "24,d1,null,25.0,",
          "25,d1,null,26.0,",
          "26,d1,null,27.0,",
          "27,d1,null,28.0,",
          "28,d1,null,29.0,",
          "29,d1,null,30.0,",
          "30,d1,null,31.0,",
          "16,d2,16,17.0,",
          "17,d2,17,18.0,",
          "18,d2,18,19.0,",
          "19,d2,19,20.0,",
          "20,d2,20,21.0,",
          "21,d2,null,22.0,",
          "22,d2,null,23.0,",
          "23,d2,null,24.0,",
          "24,d2,null,25.0,",
          "25,d2,null,26.0,",
          "26,d2,null,27.0,",
          "27,d2,null,28.0,",
          "28,d2,null,29.0,",
          "29,d2,null,30.0,",
          "30,d2,null,31.0,",
        };
    tableResultSetEqualTest(
        "select Time,Device,s2, s3 + 1 from table0 where s3 + 1 > 16 order by device",
        expectedHeader,
        retArray,
        database);
  }

  @Ignore
  @Test
  public void testAggregationAlignByDevice1() {
    String[] expectedHeader = new String[] {"Device,count(s2),sum(s3),"};
    String[] retArray = new String[] {"d2,11,165.0,", "d1,10,152.0,"};
    tableResultSetEqualTest(
        "select count(s2), sum(s3) from d1, d2 where s2 - 1 >= 9 and s2 < 30 order by device desc align by device",
        expectedHeader,
        retArray,
        database);
  }

  @Ignore
  @Test
  public void testAggregationAlignByDevice2() {
    String[] expectedHeader = new String[] {"Device,count(s2),sum(s3 + 1),"};
    String[] retArray = new String[] {"d1,6,390339.0,", "d2,5,360.0,"};
    tableResultSetEqualTest(
        "select count(s2), sum(s3 + 1) from d1, d2 where s3 + 1 > 16 align by device",
        expectedHeader,
        retArray,
        database);
  }

  @Ignore
  @Test
  public void testGroupByTimeAlignByDevice1() {
    String[] expectedHeader = new String[] {"Time,Device,count(s2),sum(s3),"};
    String[] retArray =
        new String[] {
          "1,d2,1,10.0,",
          "11,d2,10,155.0,",
          "21,d2,0,null,",
          "31,d2,0,null,",
          "1,d1,1,10.0,",
          "11,d1,9,142.0,",
          "21,d1,0,null,",
          "31,d1,0,null,"
        };
    tableResultSetEqualTest(
        "select count(s2), sum(s3) from d1, d2 where s2 - 1 >= 9 and s2 < 30 group by ([1, 41), 10ms) order by device desc align by device",
        expectedHeader,
        retArray,
        database);
  }

  @Ignore
  @Test
  public void testGroupByTimeAlignByDevice2() {
    String[] expectedHeader = new String[] {"Time,Device,count(s2),sum(s3 + 1),"};
    String[] retArray =
        new String[] {
          "1,d1,0,30001.0,",
          "11,d1,6,130096.0,",
          "21,d1,0,230242.0,",
          "31,d1,0,null,",
          "1,d2,0,null,",
          "11,d2,5,95.0,",
          "21,d2,0,265.0,",
          "31,d2,0,null,"
        };
    tableResultSetEqualTest(
        "select count(s2), sum(s3 + 1) from d1, d2 where s3 + 1 > 16 group by ([1, 41), 10ms) align by device",
        expectedHeader,
        retArray,
        database);
  }
}
