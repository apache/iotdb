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

package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.db.it.utils.AlignedWriteUtil;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBPredicatePushDownIT {

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
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAlignedRawDataAlignByTime1() {
    String expectedHeader1 = "Time,root.sg1.d1.s2,root.sg1.d1.s3,";
    String[] retArray1 =
        new String[] {
          "10,10,10,",
          "11,11,11,",
          "12,12,12,",
          "13,130000,130000,",
          "14,14,14,",
          "15,15,15,",
          "16,16,16,",
          "17,17,17,",
          "18,18,18,",
          "19,19,19,",
          "20,20,20,"
        };
    resultSetEqualTest(
        "select s2, s3 from root.sg1.d1 where s2 >= 10 and s2 < 30", expectedHeader1, retArray1);

    String expectedHeader2 = "Time,root.sg1.d1.s3,";
    String[] retArray2 =
        new String[] {
          "10,10,",
          "11,11,",
          "12,12,",
          "13,130000,",
          "14,14,",
          "15,15,",
          "16,16,",
          "17,17,",
          "18,18,",
          "19,19,",
          "20,20,"
        };
    resultSetEqualTest(
        "select s3 from root.sg1.d1 where s2 >= 10 and s2 < 30", expectedHeader2, retArray2);

    String expectedHeader3 = "Time,root.sg1.d1.s2,";
    String[] retArray3 =
        new String[] {
          "10,10,",
          "11,11,",
          "12,12,",
          "13,130000,",
          "14,14,",
          "15,15,",
          "16,16,",
          "17,17,",
          "18,18,",
          "19,19,",
          "20,20,"
        };
    resultSetEqualTest(
        "select s2 from root.sg1.d1 where s2 >= 10 and s2 < 30", expectedHeader3, retArray3);
  }

  @Test
  public void testAlignedRawDataAlignByTime2() {
    String expectedHeader1 = "Time,root.sg1.d1.s2,root.sg1.d1.s3,";
    String[] retArray1 =
        new String[] {
          "3,null,30000,",
          "13,130000,130000,",
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
    resultSetEqualTest("select s2, s3 from root.sg1.d1 where s3 > 15", expectedHeader1, retArray1);

    String expectedHeader2 = "Time,root.sg1.d1.s2,";
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
    resultSetEqualTest("select s2 from root.sg1.d1 where s3 > 15", expectedHeader2, retArray2);

    String expectedHeader3 = "Time,root.sg1.d1.s3,";
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
          "23,23,",
          "24,24,",
          "25,25,",
          "26,26,",
          "27,27,",
          "28,28,",
          "29,29,",
          "30,30,",
        };
    resultSetEqualTest("select s3 from root.sg1.d1 where s3 > 15", expectedHeader3, retArray3);
  }

  @Test
  public void testNonAlignedRawDataAlignByTime1() {
    String expectedHeader1 = "Time,root.sg1.d2.s2,root.sg1.d2.s3,";
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
    resultSetEqualTest(
        "select s2, s3 from root.sg1.d2 where s2 >= 10 and s2 < 30", expectedHeader1, retArray1);

    String expectedHeader2 = "Time,root.sg1.d2.s3,";
    String[] retArray2 =
        new String[] {
          "10,10,", "11,11,", "12,12,", "13,13,", "14,14,", "15,15,", "16,16,", "17,17,", "18,18,",
          "19,19,", "20,20,"
        };
    resultSetEqualTest(
        "select s3 from root.sg1.d2 where s2 >= 10 and s2 < 30", expectedHeader2, retArray2);

    String expectedHeader3 = "Time,root.sg1.d2.s2,";
    String[] retArray3 =
        new String[] {
          "10,10,", "11,11,", "12,12,", "13,13,", "14,14,", "15,15,", "16,16,", "17,17,", "18,18,",
          "19,19,", "20,20,"
        };
    resultSetEqualTest(
        "select s2 from root.sg1.d2 where s2 >= 10 and s2 < 30", expectedHeader3, retArray3);
  }

  @Test
  public void testNonAlignedRawDataAlignByTime2() {
    String expectedHeader1 = "Time,root.sg1.d2.s2,root.sg1.d2.s3,";
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
    resultSetEqualTest("select s2, s3 from root.sg1.d2 where s3 > 15", expectedHeader1, retArray1);

    String expectedHeader2 = "Time,root.sg1.d2.s2,";
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
    resultSetEqualTest("select s2 from root.sg1.d2 where s3 > 15", expectedHeader2, retArray2);

    String expectedHeader3 = "Time,root.sg1.d2.s3,";
    String[] retArray3 =
        new String[] {
          "16,16,", "17,17,", "18,18,", "19,19,", "20,20,", "21,21,", "22,22,", "23,23,", "24,24,",
          "25,25,", "26,26,", "27,27,", "28,28,", "29,29,", "30,30,",
        };
    resultSetEqualTest("select s3 from root.sg1.d2 where s3 > 15", expectedHeader3, retArray3);
  }

  @Test
  public void testMixRawDataAlignByTime() {
    String expectedHeader1 = "Time,root.sg1.d1.s2,root.sg1.d2.s2,root.sg1.d1.s3,root.sg1.d2.s3,";
    String[] retArray1 =
        new String[] {
          "10,10,10,10,10,",
          "11,11,11,11,11,",
          "12,12,12,12,12,",
          "13,130000,13,130000,13,",
          "14,14,14,14,14,",
          "15,15,15,15,15,",
          "16,16,16,16,16,",
          "17,17,17,17,17,",
          "18,18,18,18,18,",
          "19,19,19,19,19,",
          "20,20,20,20,20,"
        };
    resultSetEqualTest(
        "select s2, s3 from root.sg1.d1, root.sg1.d2 where s2 >= 10 and s2 < 30",
        expectedHeader1,
        retArray1);
  }

  @Test
  public void testAlignedAggregationAlignByTime1() {
    String expectedHeader1 = "count(root.sg1.d1.s2),count(root.sg1.d1.s3),";
    String[] retArray1 =
        new String[] {
          "11,11,",
        };
    resultSetEqualTest(
        "select count(s2), count(s3) from root.sg1.d1 where s2 >= 10 and s2 < 30",
        expectedHeader1,
        retArray1);

    String expectedHeader2 = "count(root.sg1.d1.s3),";
    String[] retArray2 = new String[] {"11,"};
    resultSetEqualTest(
        "select count(s3) from root.sg1.d1 where s2 >= 10 and s2 < 30", expectedHeader2, retArray2);

    String expectedHeader3 = "count(root.sg1.d1.s2),";
    String[] retArray3 =
        new String[] {
          "11,",
        };
    resultSetEqualTest(
        "select count(s2) from root.sg1.d1 where s2 >= 10 and s2 < 30", expectedHeader3, retArray3);
  }

  @Test
  public void testAlignedAggregationAlignByTime2() {
    String expectedHeader1 = "count(root.sg1.d1.s2),count(root.sg1.d1.s3),";
    String[] retArray1 =
        new String[] {
          "6,17,",
        };
    resultSetEqualTest(
        "select count(s2), count(s3) from root.sg1.d1 where s3 > 15", expectedHeader1, retArray1);

    String expectedHeader2 = "count(root.sg1.d1.s2),";
    String[] retArray2 =
        new String[] {
          "6,",
        };
    resultSetEqualTest(
        "select count(s2) from root.sg1.d1 where s3 > 15", expectedHeader2, retArray2);

    String expectedHeader3 = "count(root.sg1.d1.s3),";
    String[] retArray3 =
        new String[] {
          "17,",
        };
    resultSetEqualTest(
        "select count(s3) from root.sg1.d1 where s3 > 15", expectedHeader3, retArray3);
  }

  @Test
  public void testNonAlignedAggregationAlignByTime1() {
    String expectedHeader1 = "count(root.sg1.d2.s2),count(root.sg1.d2.s3),";
    String[] retArray1 = new String[] {"11,11,"};
    resultSetEqualTest(
        "select count(s2), count(s3) from root.sg1.d2 where s2 >= 10 and s2 < 30",
        expectedHeader1,
        retArray1);

    String expectedHeader2 = "count(root.sg1.d2.s3),";
    String[] retArray2 = new String[] {"11,"};
    resultSetEqualTest(
        "select count(s3) from root.sg1.d2 where s2 >= 10 and s2 < 30", expectedHeader2, retArray2);

    String expectedHeader3 = "count(root.sg1.d2.s2),";
    String[] retArray3 = new String[] {"11,"};
    resultSetEqualTest(
        "select count(s2) from root.sg1.d2 where s2 >= 10 and s2 < 30", expectedHeader3, retArray3);
  }

  @Test
  public void testNonAlignedAggregationAlignByTime2() {
    String expectedHeader1 = "count(root.sg1.d2.s2),count(root.sg1.d2.s3),";
    String[] retArray1 =
        new String[] {
          "5,15,",
        };
    resultSetEqualTest(
        "select count(s2), count(s3) from root.sg1.d2 where s3 > 15", expectedHeader1, retArray1);

    String expectedHeader2 = "count(root.sg1.d2.s2),";
    String[] retArray2 =
        new String[] {
          "5,",
        };
    resultSetEqualTest(
        "select count(s2) from root.sg1.d2 where s3 > 15", expectedHeader2, retArray2);

    String expectedHeader3 = "count(root.sg1.d2.s3),";
    String[] retArray3 =
        new String[] {
          "15,",
        };
    resultSetEqualTest(
        "select count(s3) from root.sg1.d2 where s3 > 15", expectedHeader3, retArray3);
  }

  @Test
  public void testMixAggregationAlignByTime() {
    String expectedHeader1 =
        "count(root.sg1.d1.s2),count(root.sg1.d2.s2),count(root.sg1.d1.s3),count(root.sg1.d2.s3),";
    String[] retArray1 =
        new String[] {
          "11,11,11,11,",
        };
    resultSetEqualTest(
        "select count(s2), count(s3) from root.sg1.d1, root.sg1.d2 where s2 >= 10 and s2 < 30",
        expectedHeader1,
        retArray1);
  }
}
