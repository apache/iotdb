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

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.NULL;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByLevelQueryIT {

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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg2");
      statement.execute(
          "create aligned timeseries root.sg2.d1(s1 FLOAT encoding=RLE, s2 INT32 encoding=Gorilla compression=SNAPPY, s3 INT64)");
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "insert into root.sg2.d1(time, s1) aligned values(%d,%f)", i, (double) i));
      }
      for (int i = 11; i <= 20; i++) {
        statement.execute(
            String.format("insert into root.sg2.d1(time, s2) aligned values(%d,%d)", i, i));
      }
      for (int i = 21; i <= 30; i++) {
        statement.execute(
            String.format("insert into root.sg2.d1(time, s3) aligned values(%d,%d)", i, i));
      }
      for (int i = 31; i <= 40; i++) {
        statement.execute(
            String.format(
                "insert into root.sg2.d1(time, s1, s2, s3) aligned values(%d,%f,%d,%d)",
                i, (double) i, i, i));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void countFuncByLevelTest() {
    // level = 1
    double[][] retArray1 = new double[][] {{39, 20}};
    String[] columnNames1 = {"count(root.sg1.*.s1)", "count(root.sg2.*.s1)"};
    resultSetEqualTest("select count(s1) from root.*.* group by level=1", retArray1, columnNames1);
    // level = 2
    double[][] retArray2 = new double[][] {{40, 19}};
    String[] columnNames2 = {"count(root.*.d1.s1)", "count(root.*.d2.s1)"};
    // level = 3
    resultSetEqualTest("select count(s1) from root.*.* group by level=2", retArray2, columnNames2);
    double[][] retArray3 = new double[][] {{59}};
    String[] columnNames3 = {"count(root.*.*.s1)"};
    resultSetEqualTest("select count(s1) from root.*.* group by level=3", retArray3, columnNames3);
    // multi level = 1,2
    double[][] retArray4 = new double[][] {{20, 19, 20}};
    String[] columnNames4 = {
      "count(root.sg1.d1.s1)", "count(root.sg1.d2.s1)", "count(root.sg2.d1.s1)"
    };
    resultSetEqualTest(
        "select count(s1) from root.*.* group by level=1,2", retArray4, columnNames4);
    // level=2 with time filter
    double[][] retArray5 =
        new double[][] {
          {17, 8},
          {10, 10},
          {1, 0},
          {10, 0}
        };
    String[] columnNames5 = {"count(root.*.d1.s1)", "count(root.*.d2.s1)"};
    resultSetEqualTest(
        "select count(s1) from root.*.* where time>=2 group by ([1,41),10ms), level=2",
        retArray5,
        columnNames5);
  }

  @Test
  public void sumFuncByLevelTest() {
    // level = 1
    double[][] retArray1 = new double[][] {{131111, 510}};
    String[] columnNames1 = {"sum(root.sg1.*.s2)", "sum(root.sg2.*.s2)"};
    resultSetEqualTest("select sum(s2) from root.*.* group by level=1", retArray1, columnNames1);
    // level = 2
    double[][] retArray2 = new double[][] {{131059, 562}};
    String[] columnNames2 = {"sum(root.*.d1.s2)", "sum(root.*.d2.s2)"};
    // level = 3
    resultSetEqualTest("select sum(s2) from root.*.* group by level=2", retArray2, columnNames2);
    double[][] retArray3 = new double[][] {{131621}};
    String[] columnNames3 = {"sum(root.*.*.s2)"};
    resultSetEqualTest("select sum(s2) from root.*.* group by level=3", retArray3, columnNames3);
    // multi level = 1,2
    double[][] retArray4 = new double[][] {{130549, 562, 510}};
    String[] columnNames4 = {"sum(root.sg1.d1.s2)", "sum(root.sg1.d2.s2)", "sum(root.sg2.d1.s2)"};
    resultSetEqualTest("select sum(s2) from root.*.* group by level=1,2", retArray4, columnNames4);
    // level=2 with time filter
    double[][] retArray5 =
        new double[][] {
          {51, 51},
          {130297, 155},
          {NULL, NULL},
          {710, 355}
        };
    String[] columnNames5 = {"sum(root.*.d1.s2)", "sum(root.*.d2.s2)"};
    resultSetEqualTest(
        "select sum(s2) from root.*.* where time>=2 group by ([1,41),10ms), level=2",
        retArray5,
        columnNames5);
  }

  @Test
  public void avgFuncByLevelTest() {
    // level = 1
    double[][] retArray1 = new double[][] {{2260.53448275862, 25.5}};
    String[] columnNames1 = {"avg(root.sg1.*.s2)", "avg(root.sg2.*.s2)"};
    resultSetEqualTest("select avg(s2) from root.*.* group by level=1", retArray1, columnNames1);
    // level = 2
    double[][] retArray2 = new double[][] {{2674.6734693877547, 19.379310344827587}};
    String[] columnNames2 = {"avg(root.*.d1.s2)", "avg(root.*.d2.s2)"};
    // level = 3
    resultSetEqualTest("select avg(s2) from root.*.* group by level=2", retArray2, columnNames2);
    double[][] retArray3 = new double[][] {{1687.4487179487176}};
    String[] columnNames3 = {"avg(root.*.*.s2)"};
    resultSetEqualTest("select avg(s2) from root.*.* group by level=3", retArray3, columnNames3);
    // multi level = 1,2
    double[][] retArray4 = new double[][] {{4501.68965517241, 19.379310344827587, 25.5}};
    String[] columnNames4 = {"avg(root.sg1.d1.s2)", "avg(root.sg1.d2.s2)", "avg(root.sg2.d1.s2)"};
    resultSetEqualTest("select avg(s2) from root.*.* group by level=1,2", retArray4, columnNames4);
    //     level=2 with time filter
    double[][] retArray5 =
        new double[][] {
          {6.375, 6.375},
          {6514.85, 15.5},
          {NULL, NULL},
          {35.5, 35.5}
        };
    String[] columnNames5 = {"avg(root.*.d1.s2)", "avg(root.*.d2.s2)"};
    resultSetEqualTest(
        "select avg(s2) from root.*.* where time>=2 group by ([1,41),10ms), level=2",
        retArray5,
        columnNames5);
  }

  @Test
  public void timeFuncGroupByLevelTest() {
    double[][] retArray1 = new double[][] {{1, 40, 1, 30}};
    String[] columnNames1 = {
      "min_time(root.*.d1.s3)",
      "max_time(root.*.d1.s3)",
      "min_time(root.*.d2.s3)",
      "max_time(root.*.d2.s3)"
    };
    resultSetEqualTest(
        "select min_time(s3),max_time(s3) from root.*.* group by level=2", retArray1, columnNames1);
  }

  @Test
  public void valueFuncGroupByLevelTest() {
    double[][] retArray1 = new double[][] {{40, 230000, 30, 30}};
    String[] columnNames1 = {
      "last_value(root.*.d1.s3)",
      "max_value(root.*.d1.s3)",
      "last_value(root.*.d2.s3)",
      "max_value(root.*.d2.s3)"
    };
    resultSetEqualTest(
        "select last_value(s3),max_value(s3) from root.*.* group by level=2",
        retArray1,
        columnNames1);
  }

  @Test
  public void nestedQueryTest1() {
    // level = 1
    double[][] retArray1 = new double[][] {{40.0, 21.0}};
    String[] columnNames1 = {"count(root.sg1.*.s1 + 1) + 1", "count(root.sg2.*.s1 + 1) + 1"};
    resultSetEqualTest(
        "select count(s1 + 1) + 1 from root.*.* group by level=1", retArray1, columnNames1);

    // level = 2
    double[][] retArray2 = new double[][] {{41.0, 20.0}};
    String[] columnNames2 = {"count(root.*.d1.s1 + 1) + 1", "count(root.*.d2.s1 + 1) + 1"};
    resultSetEqualTest(
        "select count(s1 + 1) + 1 from root.*.* group by level=2", retArray2, columnNames2);

    // level = 3
    double[][] retArray3 = new double[][] {{60.0}};
    String[] columnNames3 = {"count(root.*.*.s1 + 1) + 1"};
    resultSetEqualTest(
        "select count(s1 + 1) + 1 from root.*.* group by level=3", retArray3, columnNames3);
  }

  @Test
  public void nestedQueryTest2() {
    // level = 1
    double[][] retArray1 = new double[][] {{390423.0, 449.0, 390404.0, 430.0}};
    String[] columnNames1 = {
      "count(root.sg1.*.s1) + sum(root.sg1.*.s1)",
      "count(root.sg1.*.s1) + sum(root.sg2.*.s1)",
      "count(root.sg2.*.s1) + sum(root.sg1.*.s1)",
      "count(root.sg2.*.s1) + sum(root.sg2.*.s1)"
    };
    resultSetEqualTest(
        "select count(s1) + sum(s1) from root.*.* group by level=1", retArray1, columnNames1);

    // level = 2
    double[][] retArray2 = new double[][] {{390634.0, 240.0, 390613.0, 219.0}};
    String[] columnNames2 = {
      "count(root.*.d1.s1) + sum(root.*.d1.s1)",
      "count(root.*.d1.s1) + sum(root.*.d2.s1)",
      "count(root.*.d2.s1) + sum(root.*.d1.s1)",
      "count(root.*.d2.s1) + sum(root.*.d2.s1)"
    };
    resultSetEqualTest(
        "select count(s1) + sum(s1) from root.*.* group by level=2", retArray2, columnNames2);

    // level = 3
    double[][] retArray3 = new double[][] {{390853.0}};
    String[] columnNames3 = {"count(root.*.*.s1) + sum(root.*.*.s1)"};
    resultSetEqualTest(
        "select count(s1) + sum(s1) from root.*.* group by level=3", retArray3, columnNames3);
  }

  @Test
  public void caseSensitivityTest() {
    double[][] retArray = new double[][] {{39, 20, 39, 20, 39, 20}};

    String[] columnNames1 = {
      "count(root.sg1.*.s1)",
      "count(root.sg2.*.s1)",
      "COUNT(root.sg1.*.s1)",
      "COUNT(root.sg2.*.s1)",
      "cOuNt(root.sg1.*.s1)",
      "cOuNt(root.sg2.*.s1)"
    };
    resultSetEqualTest(
        "select count(s1), COUNT(s1), cOuNt(s1) from root.*.* group by level=1",
        retArray,
        columnNames1);

    String[] columnNames2 = {
      "Count(root.sg1.*.s1)",
      "Count(root.sg2.*.s1)",
      "COUNT(root.sg1.*.s1)",
      "COUNT(root.sg2.*.s1)",
      "cOuNt(root.sg1.*.s1)",
      "cOuNt(root.sg2.*.s1)"
    };
    resultSetEqualTest(
        "select Count(s1), COUNT(s1), cOuNt(s1) from root.*.* group by level=1",
        retArray,
        columnNames2);
  }
}
