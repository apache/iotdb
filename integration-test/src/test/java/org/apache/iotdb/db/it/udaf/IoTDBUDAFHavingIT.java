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

package org.apache.iotdb.db.it.udaf;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.*;
import static org.apache.iotdb.itbase.constant.TestConstant.*;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFHavingIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.test",
        "CREATE TIMESERIES root.test.sg1.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s4 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s4 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE ALIGNED TIMESERIES root.test.sg3(s5 INT32, s6 BOOLEAN, s7 DOUBLE, s8 INT32)",
        "CREATE TIMESERIES root.test.sg5.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg5.s9 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) values(1, true, 1, 1.0, 1)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) values(1, false, 1, 1.0, 1)",
        "INSERT INTO root.test.sg1(timestamp, s2) values(2, 2)",
        "INSERT INTO root.test.sg1(timestamp, s3) values(2, 2.0)",
        "INSERT INTO root.test.sg1(timestamp, s4) values(2, 2)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) values(2, true, 2, 2.0, 2)",
        "INSERT INTO root.test.sg1(timestamp, s1) values(3, false)",
        "INSERT INTO root.test.sg1(timestamp, s2) values(5, 5)",
        "INSERT INTO root.test.sg1(timestamp, s3) values(5, 5.0)",
        "INSERT INTO root.test.sg1(timestamp, s4) values(5, 5)",
        "INSERT INTO root.test.sg2(timestamp, s2) values(5, 5)",
        "INSERT INTO root.test.sg2(timestamp, s3) values(5, 5.0)",
        "INSERT INTO root.test.sg2(timestamp, s4) values(5, 5)",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) values(6, true, 6, 6.0, 6)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) values(6, true, 6, 6.0, 6)",
        "INSERT INTO root.test.sg1(timestamp, s1) values(7, true)",
        "INSERT INTO root.test.sg1(timestamp, s3) values(7, 7.0)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) values(7, true, 7, 7.0)",
        "INSERT INTO root.test.sg1(timestamp, s1) values(8, true)",
        "INSERT INTO root.test.sg1(timestamp, s2) values(8, 8)",
        "INSERT INTO root.test.sg1(timestamp, s3) values(8, 8.0)",
        "INSERT INTO root.test.sg2(timestamp, s3) values(8, 8.0)",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) values(9, false, 9, 9.0, 9)",
        "INSERT INTO root.test.sg2(timestamp, s1) values(9, true)",
        "INSERT INTO root.test.sg2(timestamp, s2) values(9, 9)",
        "INSERT INTO root.test.sg2(timestamp, s4) values(9, 9)",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) values(10, true, 10, 10.0, 10)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) values(10, true, 10, 10.0, 10)",
        "INSERT INTO root.test.sg3(time, s5, s6, s7, s8) aligned values(1, 1, true, 1.0, 1)",
        "INSERT INTO root.test.sg3(time, s6, s7, s8) aligned values(2, false, 2.0, 2)",
        "INSERT INTO root.test.sg3(time, s5, s7, s8) aligned values(3, 3, 3.0, 3)",
        "INSERT INTO root.test.sg3(time, s5) aligned values(5, 5)",
        "INSERT INTO root.test.sg3(time, s5, s8) aligned values(6, 6, 6)",
        "INSERT INTO root.test.sg3(time, s6) aligned values(7, true)",
        "INSERT INTO root.test.sg3(time, s5, s7) aligned values(8, 8, 8.0)",
        "INSERT INTO root.test.sg3(time, s5, s7, s8) aligned values(9, 9, 9.0, 9)",
        "INSERT INTO root.test.sg3(time, s7, s8) aligned values(10, 10.0, 10)",
        "INSERT INTO root.test.sg5(timestamp, s1, s9) values(1, true, 1)",
        "CREATE TIMESERIES root.test1.d1.code TEXT",
        "CREATE TIMESERIES root.test1.d1.tem INT32",
        "INSERT INTO root.test1.d1(timestamp, code, tem) values(1, '123', 345);",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
    registerUDAF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void registerUDAF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION avg_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFAvg'");
      statement.execute(
          "CREATE FUNCTION count_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      statement.execute(
          "CREATE FUNCTION sum_udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFSum'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void unsatisfiedRuleQueryTest() {
    assertTestFail(
        "SELECT count_udaf(s1) FROM root.** GROUP BY ([1,3),1ms), LEVEL=1 HAVING sum_udaf(d1.s1) > 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": When Having used with GroupByLevel: the suffix paths can only be measurement or one-level wildcard");

    assertTestFail(
        "SELECT count_udaf(d1.s1) FROM root.** GROUP BY ([1,3),1ms), LEVEL=1 HAVING sum_udaf(s1) > 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": When Having used with GroupByLevel: the suffix paths can only be measurement or one-level wildcard");

    assertTestFail(
        "SELECT count_udaf(d1.s1) FROM root.** GROUP BY ([1,3),1ms), LEVEL=1 HAVING sum_udaf(s1) + s1 > 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Raw data and aggregation result hybrid calculation is not supported");

    assertTestFail(
        "SELECT count_udaf(d1.s1) FROM root.** GROUP BY ([1,3),1ms), LEVEL=1 HAVING s1 + 1 > 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Expression of HAVING clause must to be an Aggregation");
  }

  @Test
  public void UDAFGroupByTimeWithHavingTest() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, countUDAF("root.test.sg1.s1")};
    String[] retArray = new String[] {"1,1,", "5,1,", "9,2,"};
    resultSetEqualWithDescOrderTest(
        "SELECT count_udaf(sg1.s1) "
            + "FROM root.** "
            + "GROUP BY ([1,11), 2ms) "
            + "HAVING count_udaf(sg1.s2) > 1",
        expectedHeader,
        retArray);

    resultSetEqualWithDescOrderTest(
        "SELECT count_udaf(sg1.s1) "
            + "FROM root.**"
            + "GROUP BY ([1,11), 2ms) "
            + "HAVING count_udaf(sg1.s2) > 2",
        expectedHeader,
        new String[] {});
  }

  @Test
  public void UDAFGroupByTimeAlignByDeviceWithHavingTest() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, "Device", countUDAF("s1"), countUDAF("s2")};
    String[] retArray =
        new String[] {
          "1,root.test.sg1,1,2,",
          "5,root.test.sg1,1,2,",
          "9,root.test.sg1,2,2,",
          "1,root.test.sg2,2,2,",
          "5,root.test.sg2,1,2,",
          "9,root.test.sg2,2,2,",
        };
    resultSetEqualTest(
        "SELECT count_udaf(s1), count_udaf(s2) "
            + "FROM root.** "
            + "GROUP BY ([1,11), 2ms) "
            + "HAVING count_udaf(s2) > 1 "
            + "ALIGN BY DEVICE",
        expectedHeader,
        retArray);

    resultSetEqualTest(
        "SELECT count_udaf(s1), count_udaf(s2) "
            + "FROM root.** "
            + "GROUP BY ([1,11), 2ms) "
            + "HAVING count_udaf(s2) > 2 "
            + "ALIGN BY DEVICE",
        expectedHeader,
        new String[] {});
  }

  @Test
  public void UDAFGroupByLevelWithHavingTest() {
    String[] expectedHeader =
        new String[] {
          countUDAF("root.test.*.s1"), countUDAF("root.test.*.s2"), sumUDAF("root.test.*.s3")
        };
    String[] retArray = new String[] {"14,14,87.0,"};
    resultSetEqualWithDescOrderTest(
        "SELECT count_udaf(s1), count_udaf(s2), sum_udaf(s3) "
            + "FROM root.** "
            + "GROUP BY LEVEL=1 "
            + "HAVING sum_udaf(s3) > 80",
        expectedHeader,
        retArray);

    resultSetEqualWithDescOrderTest(
        "SELECT count_udaf(s1), count_udaf(s2), sum_udaf(s3) "
            + "FROM root.** "
            + "GROUP BY LEVEL=1 "
            + "HAVING sum_udaf(s3) > 90",
        expectedHeader,
        new String[] {});
  }

  @Test
  public void UDAFGroupByTimeLevelWithHavingTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, countUDAF("root.test.*.s1"), countUDAF("root.test.*.s2"),
        };
    String[] retArray =
        new String[] {
          "1,4,4,", "5,2,4,", "9,4,4,",
        };
    resultSetEqualWithDescOrderTest(
        "SELECT count_udaf(s1), count_udaf(s2) "
            + "FROM root.** "
            + "GROUP BY ([1,11),2ms), LEVEL=1 "
            + "HAVING count_udaf(s2)>2",
        expectedHeader,
        retArray);

    resultSetEqualWithDescOrderTest(
        "SELECT count_udaf(s1), count_udaf(s2) "
            + "FROM root.** "
            + "GROUP BY ([1,11),2ms), LEVEL=1 "
            + "HAVING count_udaf(s2)>4",
        expectedHeader,
        new String[] {});
  }

  @Test
  public void UDAFGroupByTimeLevelWithHavingLimitTest() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, countUDAF("root.test.*.s1"), countUDAF("root.test.*.s2"),
        };
    String[] retArray = new String[] {"5,2,4,"};
    resultSetEqualTest(
        "SELECT count_udaf(s1), count_udaf(s2) "
            + "FROM root.** "
            + "GROUP BY ([1,11),2ms), LEVEL=1 "
            + "HAVING count_udaf(s2) > 1 "
            + "LIMIT 1 OFFSET 1",
        expectedHeader,
        retArray);
  }

  @Test
  public void UDAFSameConstantTest() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, avgUDAF("root.test1.d1.tem")};
    String[] retArray = new String[] {"1,345.0,"};
    resultSetEqualTest(
        "SELECT avg_udaf(tem) "
            + "FROM root.test1.d1 "
            + "WHERE code='123' "
            + "GROUP BY([0,5), 1ms) "
            + "HAVING min_value(tem) != 123",
        expectedHeader,
        retArray);
  }
}
