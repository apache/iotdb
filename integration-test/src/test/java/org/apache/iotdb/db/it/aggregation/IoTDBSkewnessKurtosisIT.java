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
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DEVICE;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.kurtosis;
import static org.apache.iotdb.itbase.constant.TestConstant.skewness;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSkewnessKurtosisIT {

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
  public void testMomentsWithUnsupportedTypesAndWrongArity() {
    String typeError =
        "Aggregate functions [SKEWNESS, KURTOSIS] only support "
            + "numeric data types [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
    String argError = "Error size of input expressions";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.executeQuery("SELECT skewness(s3) FROM root.db.d1");
        fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(typeError));
      }

      try {
        statement.executeQuery("SELECT kurtosis(s6) FROM root.db.d1");
        fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains(typeError));
      }

      try {
        statement.executeQuery("SELECT kurtosis(s1, s2) FROM root.db.d1");
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
  public void testMomentsWithDifferentTypes() {
    String[] expectedHeader =
        new String[] {
          skewness("root.db.d1.s1"),
          skewness("root.db.d1.s2"),
          kurtosis("root.db.d1.s1"),
          kurtosis("root.db.d1.s2"),
          skewness("root.db.d1.s4"),
          skewness("root.db.d1.s5"),
          kurtosis("root.db.d1.s4"),
          kurtosis("root.db.d1.s5")
        };
    String[] retArray =
        new String[] {
          "0.0,0.4082482904638631,-1.2000000000000002,-3.333333333333333,0.0,0.4082482904638631,-1.2000000000000002,-3.333333333333333,"
        };
    resultSetEqualTest(
        "select skewness(s1),skewness(s2),kurtosis(s1),kurtosis(s2),"
            + "skewness(s4),skewness(s5),kurtosis(s4),kurtosis(s5) from root.db.d1",
        expectedHeader,
        retArray);

    expectedHeader =
        new String[] {
          skewness("root.db.d1.s1"), skewness("root.db.d1.s2"), kurtosis("root.db.d1.s1")
        };
    retArray = new String[] {"0.0,-0.7071067811865475,null,"};
    resultSetEqualTest(
        "select skewness(s1),skewness(s2),kurtosis(s1) from root.db.d1 where time < 10",
        expectedHeader,
        retArray);
  }

  @Test
  public void testMomentsAlignByDevice() {
    String[] expectedHeader =
        new String[] {
          DEVICE, skewness("s1"), skewness("s2"), kurtosis("s1"), kurtosis("s2"),
        };
    String[] retArray =
        new String[] {"root.db.d1,0.0,0.4082482904638631,-1.2000000000000002,-3.333333333333333,"};
    resultSetEqualTest(
        "select skewness(s1),skewness(s2),kurtosis(s1),kurtosis(s2) from root.db.d1 align by device",
        expectedHeader,
        retArray);

    retArray = new String[] {"root.db.d1,0.0,-0.7071067811865475,null,null,"};
    resultSetEqualTest(
        "select skewness(s1),skewness(s2),kurtosis(s1),kurtosis(s2) from root.db.d1 "
            + "where time < 10 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testMomentsInHaving() {
    String[] expectedHeader = new String[] {skewness("root.db.d1.s2"), kurtosis("root.db.d1.s2")};
    String[] retArray = new String[] {"0.4082482904638631,-3.333333333333333,"};
    resultSetEqualTest(
        "select skewness(s2),kurtosis(s2) from root.db.d1 having skewness(s2) > 0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testMomentsWithGroupByLevel() {
    String[] expectedHeader =
        new String[] {
          skewness("root.*.*.s1"),
          kurtosis("root.*.*.s1"),
          skewness("root.*.*.s2"),
          kurtosis("root.*.*.s2")
        };
    String[] retArray =
        new String[] {
          "1.0606601717798214,0.25714285714285623,-0.8728715609439697,-1.224489795918367,"
        };
    resultSetEqualTest(
        "select skewness(s1),kurtosis(s1),skewness(s2),kurtosis(s2) from root.db.* group by level = 0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testMomentsWithSlidingWindow() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, skewness("root.db.d1.s1"), kurtosis("root.db.d1.s1")};
    String[] retArray = new String[] {"1,0.0,null,", "3,null,null,"};
    resultSetEqualTest(
        "select skewness(s1),kurtosis(s1) from root.db.d1 group by time([1,4),3ms,2ms)",
        expectedHeader,
        retArray);
  }
}
