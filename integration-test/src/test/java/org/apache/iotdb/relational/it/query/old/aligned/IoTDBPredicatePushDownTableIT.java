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
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
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
          "1970-01-01T00:00:00.010Z,10,10,",
          "1970-01-01T00:00:00.011Z,11,11,",
          "1970-01-01T00:00:00.012Z,12,12,",
          "1970-01-01T00:00:00.014Z,14,14,",
          "1970-01-01T00:00:00.015Z,15,15,",
          "1970-01-01T00:00:00.016Z,16,16,",
          "1970-01-01T00:00:00.017Z,17,17,",
          "1970-01-01T00:00:00.018Z,18,18,",
          "1970-01-01T00:00:00.019Z,19,19,",
          "1970-01-01T00:00:00.020Z,20,20,"
        };
    tableResultSetEqualTest(
        "select Time,s2, s3 from table0 where device='d1' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader1,
        retArray1,
        database);

    String[] expectedHeader2 = new String[] {"Time", "s3"};
    String[] retArray2 =
        new String[] {
          "1970-01-01T00:00:00.010Z,10,",
          "1970-01-01T00:00:00.011Z,11,",
          "1970-01-01T00:00:00.012Z,12,",
          "1970-01-01T00:00:00.014Z,14,",
          "1970-01-01T00:00:00.015Z,15,",
          "1970-01-01T00:00:00.016Z,16,",
          "1970-01-01T00:00:00.017Z,17,",
          "1970-01-01T00:00:00.018Z,18,",
          "1970-01-01T00:00:00.019Z,19,",
          "1970-01-01T00:00:00.020Z,20,"
        };
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d1' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader2,
        retArray2,
        database);

    String[] expectedHeader3 = new String[] {"Time", "s2"};
    String[] retArray3 =
        new String[] {
          "1970-01-01T00:00:00.010Z,10,",
          "1970-01-01T00:00:00.011Z,11,",
          "1970-01-01T00:00:00.012Z,12,",
          "1970-01-01T00:00:00.014Z,14,",
          "1970-01-01T00:00:00.015Z,15,",
          "1970-01-01T00:00:00.016Z,16,",
          "1970-01-01T00:00:00.017Z,17,",
          "1970-01-01T00:00:00.018Z,18,",
          "1970-01-01T00:00:00.019Z,19,",
          "1970-01-01T00:00:00.020Z,20,"
        };
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d1' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader3,
        retArray3,
        database);

    String[] expectedHeader4 = new String[] {"Time", "s2"};
    String[] retArray4 =
        new String[] {"1970-01-01T00:00:00.014Z,14,", "1970-01-01T00:00:00.015Z,15,"};
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
          "1970-01-01T00:00:00.003Z,null,30001,",
          "1970-01-01T00:00:00.013Z,130000,130001,",
          "1970-01-01T00:00:00.016Z,16,17,",
          "1970-01-01T00:00:00.017Z,17,18,",
          "1970-01-01T00:00:00.018Z,18,19,",
          "1970-01-01T00:00:00.019Z,19,20,",
          "1970-01-01T00:00:00.020Z,20,21,",
          "1970-01-01T00:00:00.021Z,null,22,",
          "1970-01-01T00:00:00.022Z,null,23,",
          "1970-01-01T00:00:00.023Z,null,230001,",
          "1970-01-01T00:00:00.024Z,null,25,",
          "1970-01-01T00:00:00.025Z,null,26,",
          "1970-01-01T00:00:00.026Z,null,27,",
          "1970-01-01T00:00:00.027Z,null,28,",
          "1970-01-01T00:00:00.028Z,null,29,",
          "1970-01-01T00:00:00.029Z,null,30,",
          "1970-01-01T00:00:00.030Z,null,31,",
        };
    tableResultSetEqualTest(
        "select Time,s2, s3 + 1 from table0 where device='d1' and s3 + 1 > 16",
        expectedHeader1,
        retArray1,
        database);

    String[] expectedHeader2 = new String[] {"Time", "s2"};
    String[] retArray2 =
        new String[] {
          "1970-01-01T00:00:00.003Z,null,",
          "1970-01-01T00:00:00.013Z,130000,",
          "1970-01-01T00:00:00.016Z,16,",
          "1970-01-01T00:00:00.017Z,17,",
          "1970-01-01T00:00:00.018Z,18,",
          "1970-01-01T00:00:00.019Z,19,",
          "1970-01-01T00:00:00.020Z,20,",
          "1970-01-01T00:00:00.021Z,null,",
          "1970-01-01T00:00:00.022Z,null,",
          "1970-01-01T00:00:00.023Z,null,",
          "1970-01-01T00:00:00.024Z,null,",
          "1970-01-01T00:00:00.025Z,null,",
          "1970-01-01T00:00:00.026Z,null,",
          "1970-01-01T00:00:00.027Z,null,",
          "1970-01-01T00:00:00.028Z,null,",
          "1970-01-01T00:00:00.029Z,null,",
          "1970-01-01T00:00:00.030Z,null,",
        };
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d1' and s3 + 1 > 16",
        expectedHeader2,
        retArray2,
        database);

    String[] expectedHeader3 = new String[] {"Time", "s3"};
    String[] retArray3 =
        new String[] {
          "1970-01-01T00:00:00.003Z,30000,",
          "1970-01-01T00:00:00.013Z,130000,",
          "1970-01-01T00:00:00.016Z,16,",
          "1970-01-01T00:00:00.017Z,17,",
          "1970-01-01T00:00:00.018Z,18,",
          "1970-01-01T00:00:00.019Z,19,",
          "1970-01-01T00:00:00.020Z,20,",
          "1970-01-01T00:00:00.021Z,21,",
          "1970-01-01T00:00:00.022Z,22,",
          "1970-01-01T00:00:00.023Z,230000,",
          "1970-01-01T00:00:00.024Z,24,",
          "1970-01-01T00:00:00.025Z,25,",
          "1970-01-01T00:00:00.026Z,26,",
          "1970-01-01T00:00:00.027Z,27,",
          "1970-01-01T00:00:00.028Z,28,",
          "1970-01-01T00:00:00.029Z,29,",
          "1970-01-01T00:00:00.030Z,30,",
        };
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d1' and s3 + 1 > 16",
        expectedHeader3,
        retArray3,
        database);

    String[] expectedHeader4 = new String[] {"Time", "s3"};
    String[] retArray4 =
        new String[] {
          "1970-01-01T00:00:00.003Z,30000,",
          "1970-01-01T00:00:00.013Z,130000,",
          "1970-01-01T00:00:00.016Z,16,"
        };
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
          "1970-01-01T00:00:00.010Z,10,10,",
          "1970-01-01T00:00:00.011Z,11,11,",
          "1970-01-01T00:00:00.012Z,12,12,",
          "1970-01-01T00:00:00.013Z,13,13,",
          "1970-01-01T00:00:00.014Z,14,14,",
          "1970-01-01T00:00:00.015Z,15,15,",
          "1970-01-01T00:00:00.016Z,16,16,",
          "1970-01-01T00:00:00.017Z,17,17,",
          "1970-01-01T00:00:00.018Z,18,18,",
          "1970-01-01T00:00:00.019Z,19,19,",
          "1970-01-01T00:00:00.020Z,20,20,"
        };
    tableResultSetEqualTest(
        "select Time,s2, s3 from table0 where device='d2' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader1,
        retArray1,
        database);

    String[] expectedHeader2 = new String[] {"Time", "s3"};
    String[] retArray2 =
        new String[] {
          "1970-01-01T00:00:00.010Z,10,",
          "1970-01-01T00:00:00.011Z,11,",
          "1970-01-01T00:00:00.012Z,12,",
          "1970-01-01T00:00:00.013Z,13,",
          "1970-01-01T00:00:00.014Z,14,",
          "1970-01-01T00:00:00.015Z,15,",
          "1970-01-01T00:00:00.016Z,16,",
          "1970-01-01T00:00:00.017Z,17,",
          "1970-01-01T00:00:00.018Z,18,",
          "1970-01-01T00:00:00.019Z,19,",
          "1970-01-01T00:00:00.020Z,20,"
        };
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d2' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader2,
        retArray2,
        database);

    String[] expectedHeader3 = new String[] {"Time", "s2"};
    String[] retArray3 =
        new String[] {
          "1970-01-01T00:00:00.010Z,10,",
          "1970-01-01T00:00:00.011Z,11,",
          "1970-01-01T00:00:00.012Z,12,",
          "1970-01-01T00:00:00.013Z,13,",
          "1970-01-01T00:00:00.014Z,14,",
          "1970-01-01T00:00:00.015Z,15,",
          "1970-01-01T00:00:00.016Z,16,",
          "1970-01-01T00:00:00.017Z,17,",
          "1970-01-01T00:00:00.018Z,18,",
          "1970-01-01T00:00:00.019Z,19,",
          "1970-01-01T00:00:00.020Z,20,"
        };
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d2' and s2 - 1 >= 9 and s2 < 30",
        expectedHeader3,
        retArray3,
        database);

    String[] expectedHeader4 = new String[] {"Time", "s2"};
    String[] retArray4 =
        new String[] {
          "1970-01-01T00:00:00.012Z,12,",
          "1970-01-01T00:00:00.013Z,13,",
          "1970-01-01T00:00:00.014Z,14,"
        };
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
          "1970-01-01T00:00:00.016Z,16,16,",
          "1970-01-01T00:00:00.017Z,17,17,",
          "1970-01-01T00:00:00.018Z,18,18,",
          "1970-01-01T00:00:00.019Z,19,19,",
          "1970-01-01T00:00:00.020Z,20,20,",
          "1970-01-01T00:00:00.021Z,null,21,",
          "1970-01-01T00:00:00.022Z,null,22,",
          "1970-01-01T00:00:00.023Z,null,23,",
          "1970-01-01T00:00:00.024Z,null,24,",
          "1970-01-01T00:00:00.025Z,null,25,",
          "1970-01-01T00:00:00.026Z,null,26,",
          "1970-01-01T00:00:00.027Z,null,27,",
          "1970-01-01T00:00:00.028Z,null,28,",
          "1970-01-01T00:00:00.029Z,null,29,",
          "1970-01-01T00:00:00.030Z,null,30,",
        };
    tableResultSetEqualTest(
        "select Time,s2, s3 from table0 where device='d2' and s3 + 1 > 16",
        expectedHeader1,
        retArray1,
        database);

    String[] expectedHeader2 = new String[] {"Time", "s2"};
    String[] retArray2 =
        new String[] {
          "1970-01-01T00:00:00.016Z,16,",
          "1970-01-01T00:00:00.017Z,17,",
          "1970-01-01T00:00:00.018Z,18,",
          "1970-01-01T00:00:00.019Z,19,",
          "1970-01-01T00:00:00.020Z,20,",
          "1970-01-01T00:00:00.021Z,null,",
          "1970-01-01T00:00:00.022Z,null,",
          "1970-01-01T00:00:00.023Z,null,",
          "1970-01-01T00:00:00.024Z,null,",
          "1970-01-01T00:00:00.025Z,null,",
          "1970-01-01T00:00:00.026Z,null,",
          "1970-01-01T00:00:00.027Z,null,",
          "1970-01-01T00:00:00.028Z,null,",
          "1970-01-01T00:00:00.029Z,null,",
          "1970-01-01T00:00:00.030Z,null,",
        };
    tableResultSetEqualTest(
        "select Time,s2 from table0 where device='d2' and s3 + 1 > 16",
        expectedHeader2,
        retArray2,
        database);

    String[] expectedHeader3 = new String[] {"Time", "s3"};
    String[] retArray3 =
        new String[] {
          "1970-01-01T00:00:00.016Z,16,",
          "1970-01-01T00:00:00.017Z,17,",
          "1970-01-01T00:00:00.018Z,18,",
          "1970-01-01T00:00:00.019Z,19,",
          "1970-01-01T00:00:00.020Z,20,",
          "1970-01-01T00:00:00.021Z,21,",
          "1970-01-01T00:00:00.022Z,22,",
          "1970-01-01T00:00:00.023Z,23,",
          "1970-01-01T00:00:00.024Z,24,",
          "1970-01-01T00:00:00.025Z,25,",
          "1970-01-01T00:00:00.026Z,26,",
          "1970-01-01T00:00:00.027Z,27,",
          "1970-01-01T00:00:00.028Z,28,",
          "1970-01-01T00:00:00.029Z,29,",
          "1970-01-01T00:00:00.030Z,30,",
        };
    tableResultSetEqualTest(
        "select Time,s3 from table0 where device='d2' and s3 + 1 > 16",
        expectedHeader3,
        retArray3,
        database);

    String[] expectedHeader4 = new String[] {"Time", "s3"};
    String[] retArray4 =
        new String[] {
          "1970-01-01T00:00:00.026Z,26,",
          "1970-01-01T00:00:00.027Z,27,",
          "1970-01-01T00:00:00.028Z,28,",
          "1970-01-01T00:00:00.029Z,29,",
          "1970-01-01T00:00:00.030Z,30,",
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
          "1970-01-01T00:00:00.010Z,d2,10,10,",
          "1970-01-01T00:00:00.011Z,d2,11,11,",
          "1970-01-01T00:00:00.012Z,d2,12,12,",
          "1970-01-01T00:00:00.013Z,d2,13,13,",
          "1970-01-01T00:00:00.014Z,d2,14,14,",
          "1970-01-01T00:00:00.015Z,d2,15,15,",
          "1970-01-01T00:00:00.016Z,d2,16,16,",
          "1970-01-01T00:00:00.017Z,d2,17,17,",
          "1970-01-01T00:00:00.018Z,d2,18,18,",
          "1970-01-01T00:00:00.019Z,d2,19,19,",
          "1970-01-01T00:00:00.020Z,d2,20,20,",
          "1970-01-01T00:00:00.010Z,d1,10,10,",
          "1970-01-01T00:00:00.011Z,d1,11,11,",
          "1970-01-01T00:00:00.012Z,d1,12,12,",
          "1970-01-01T00:00:00.014Z,d1,14,14,",
          "1970-01-01T00:00:00.015Z,d1,15,15,",
          "1970-01-01T00:00:00.016Z,d1,16,16,",
          "1970-01-01T00:00:00.017Z,d1,17,17,",
          "1970-01-01T00:00:00.018Z,d1,18,18,",
          "1970-01-01T00:00:00.019Z,d1,19,19,",
          "1970-01-01T00:00:00.020Z,d1,20,20,",
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
          "1970-01-01T00:00:00.003Z,d1,null,30001,",
          "1970-01-01T00:00:00.013Z,d1,130000,130001,",
          "1970-01-01T00:00:00.016Z,d1,16,17,",
          "1970-01-01T00:00:00.017Z,d1,17,18,",
          "1970-01-01T00:00:00.018Z,d1,18,19,",
          "1970-01-01T00:00:00.019Z,d1,19,20,",
          "1970-01-01T00:00:00.020Z,d1,20,21,",
          "1970-01-01T00:00:00.021Z,d1,null,22,",
          "1970-01-01T00:00:00.022Z,d1,null,23,",
          "1970-01-01T00:00:00.023Z,d1,null,230001,",
          "1970-01-01T00:00:00.024Z,d1,null,25,",
          "1970-01-01T00:00:00.025Z,d1,null,26,",
          "1970-01-01T00:00:00.026Z,d1,null,27,",
          "1970-01-01T00:00:00.027Z,d1,null,28,",
          "1970-01-01T00:00:00.028Z,d1,null,29,",
          "1970-01-01T00:00:00.029Z,d1,null,30,",
          "1970-01-01T00:00:00.030Z,d1,null,31,",
          "1970-01-01T00:00:00.016Z,d2,16,17,",
          "1970-01-01T00:00:00.017Z,d2,17,18,",
          "1970-01-01T00:00:00.018Z,d2,18,19,",
          "1970-01-01T00:00:00.019Z,d2,19,20,",
          "1970-01-01T00:00:00.020Z,d2,20,21,",
          "1970-01-01T00:00:00.021Z,d2,null,22,",
          "1970-01-01T00:00:00.022Z,d2,null,23,",
          "1970-01-01T00:00:00.023Z,d2,null,24,",
          "1970-01-01T00:00:00.024Z,d2,null,25,",
          "1970-01-01T00:00:00.025Z,d2,null,26,",
          "1970-01-01T00:00:00.026Z,d2,null,27,",
          "1970-01-01T00:00:00.027Z,d2,null,28,",
          "1970-01-01T00:00:00.028Z,d2,null,29,",
          "1970-01-01T00:00:00.029Z,d2,null,30,",
          "1970-01-01T00:00:00.030Z,d2,null,31,",
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
