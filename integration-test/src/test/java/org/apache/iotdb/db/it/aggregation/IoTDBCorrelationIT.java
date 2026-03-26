/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.it.aggregation;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DEVICE;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.corr;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCorrelationIT {

  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.db",
        "CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s4 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s5 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s4 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s5 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(1, 1, 1, true, 1, 1, \"1\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(2, 2, 2, false, 2, 2, \"2\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(3, 3, 2, false, 3, 2, \"2\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(10000000000, 4, 1, true, 4, 1, \"1\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(10000000001, 5, 1, true, 5, 1, \"1\")",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(1, 1, 2, 3, 4)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(2, 1, 2, 3, 4)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(10000000000, 1, 2, 3, 4)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(10000000001, 1, 2, 3, 4)",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s4,s5) values(10000000002, 1, 2, 3, 4)",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCorrWithUnsupportedTypesAndWrongArity() {
    String typeError =
        "Aggregate functions [CORR, COVAR_POP, COVAR_SAMP, REGR_SLOPE, REGR_INTERCEPT] only support "
            + "numeric data types [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
    String argError = "Error size of input expressions";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT corr(s3, s1) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(typeError));
      }

      try {
        try (ResultSet resultSet = statement.executeQuery("SELECT corr(s6, s1) FROM root.db.d1")) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(typeError));
      }

      try {
        statement.executeQuery("SELECT corr(s1) FROM root.db.d1");
        fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(argError));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCorrWithDifferentTypes() {
    String[] expectedHeader =
        new String[] {corr("root.db.d1.s1, root.db.d1.s2"), corr("root.db.d1.s4, root.db.d1.s5")};
    String[] retArray = new String[] {"-0.28867513459481287,-0.28867513459481287,"};
    resultSetEqualTest("select corr(s1,s2),corr(s4,s5) from root.db.d1", expectedHeader, retArray);

    retArray = new String[] {"0.8660254037844386,0.8660254037844386,"};
    resultSetEqualTest(
        "select corr(s1,s2),corr(s4,s5) from root.db.d1 where time < 10", expectedHeader, retArray);
  }

  @Test
  public void testCorrAlignByDevice() {
    String[] expectedHeader = new String[] {DEVICE, corr("s1, s2"), corr("s4, s5")};
    String[] retArray = new String[] {"root.db.d1,-0.28867513459481287,-0.28867513459481287,"};
    resultSetEqualTest(
        "select corr(s1,s2),corr(s4,s5) from root.db.d1 align by device", expectedHeader, retArray);

    retArray = new String[] {"root.db.d1,0.8660254037844386,0.8660254037844386,"};
    resultSetEqualTest(
        "select corr(s1,s2),corr(s4,s5) from root.db.d1 where time < 10 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testCorrInHaving() {
    String[] expectedHeader = new String[] {corr("root.db.d1.s1, root.db.d1.s2")};
    String[] retArray = new String[] {"-0.28867513459481287,"};
    resultSetEqualTest(
        "select corr(s1,s2) from root.db.d1 having corr(s1,s2) < 0", expectedHeader, retArray);
  }

  @Test
  public void testCorrMultiDeviceWithoutGroupByLevel() {
    String[] expectedHeader =
        new String[] {
          corr("root.db.d1.s1, root.db.d1.s2"),
          corr("root.db.d1.s1, root.db.d2.s2"),
          corr("root.db.d2.s1, root.db.d1.s2"),
          corr("root.db.d2.s1, root.db.d2.s2")
        };
    String[] retArray = new String[] {"-0.28867513459481287,null,null,null,"};
    resultSetEqualTest("select corr(s1,s2) from root.db.d1,root.db.d2", expectedHeader, retArray);
  }

  @Test
  public void testCorrMultiDeviceWithGroupByLevel() {
    String[] expectedHeader = new String[] {corr("root.*.*.s1, root.*.*.s2")};
    String[] retArray = new String[] {"-0.08111071056538134,"};
    resultSetEqualTest(
        "select corr(s1,s2) from root.db.* group by level = 0", expectedHeader, retArray);
  }

  @Test
  public void testCorrWithSlidingWindow() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, corr("root.db.d1.s1, root.db.d1.s2")};
    String[] retArray = new String[] {"1,0.8660254037844387,", "3,null,"};
    resultSetEqualTest(
        "select corr(s1,s2) from root.db.d1 group by time([1,4),3ms,2ms)",
        expectedHeader,
        retArray);
  }
}
