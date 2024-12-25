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
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DEVICE;
import static org.apache.iotdb.itbase.constant.TestConstant.mode;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBModeIT {
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
        "CREATE TIMESERIES root.db.d2.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s4 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s5 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(1, 1, 1, true, 1, 1, \"1\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(2, 2, 2, false, 2, 2, \"2\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(3, 2, 2, false, 2, 2, \"2\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(10000000000, 1, 1, true, 1, 1, \"1\")",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(10000000001, 1, 1, true, 1, 1, \"1\")",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3,s4,s5,s6) values(1, 1, 1, true, 1, 1, \"1\")",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3,s4,s5,s6) values(2, 1, 1, true, 1, 1, \"1\")",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3,s4,s5,s6) values(10000000000, 2, 2, false, 2, 2, \"2\")",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3,s4,s5,s6) values(10000000001, 2, 2, false, 2, 2, \"2\")",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3,s4,s5,s6) values(10000000002, 2, 2, false, 2, 2, \"2\")",
        "INSERT INTO root.db.d2(timestamp,s1,s2,s3,s4,s5,s6) values(10000000003, 2, 2, false, 2, 2, \"2\")",
        "INSERT INTO root.test.d1(timestamp,s1) values(1, 5)",
        "INSERT INTO root.test.d1(timestamp,s1) values(2, 5)",
        "INSERT INTO root.test.d1(timestamp,s1) values(3, 2)",
        "INSERT INTO root.test.d1(timestamp,s1) values(4, 2)",
        "INSERT INTO root.test.d1(timestamp,s1) values(5, 8)",
        "INSERT INTO root.test.d1(timestamp,s1) values(6, 8)",
        "INSERT INTO root.test.d1(timestamp,s1) values(7, 1)",
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
  public void testModeWithDifferentTypes() {
    String[] expectedHeader =
        new String[] {
          mode("root.db.d1.s1"),
          mode("root.db.d1.s2"),
          mode("root.db.d1.s3"),
          mode("root.db.d1.s4"),
          mode("root.db.d1.s5"),
          mode("root.db.d1.s6"),
        };
    String[] retArray = new String[] {"1,1,true,1.0,1.0,1,"};
    resultSetEqualTest(
        "select mode(s1),mode(s2),mode(s3),mode(s4),mode(s5),mode(s6) from root.db.d1",
        expectedHeader,
        retArray);

    retArray = new String[] {"2,2,false,2.0,2.0,2,"};
    resultSetEqualTest(
        "select mode(s1),mode(s2),mode(s3),mode(s4),mode(s5),mode(s6) from root.db.d1 where time < 10",
        expectedHeader,
        retArray);
  }

  @Test
  public void testModeAlignByDevice() {
    String[] expectedHeader =
        new String[] {
          DEVICE, mode("s1"), mode("s2"), mode("s3"), mode("s4"), mode("s5"), mode("s6"),
        };
    String[] retArray = new String[] {"root.db.d1,1,1,true,1.0,1.0,1,"};
    resultSetEqualTest(
        "select mode(s1),mode(s2),mode(s3),mode(s4),mode(s5),mode(s6) from root.db.d1 align by device",
        expectedHeader,
        retArray);

    retArray = new String[] {"root.db.d1,2,2,false,2.0,2.0,2,"};
    resultSetEqualTest(
        "select mode(s1),mode(s2),mode(s3),mode(s4),mode(s5),mode(s6) from root.db.d1 where time < 10 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testModeInHaving() {
    String[] expectedHeader = new String[] {mode("root.db.d1.s1")};
    String[] retArray = new String[] {"1,"};
    resultSetEqualTest(
        "select mode(s1) from root.db.d1 having mode(s2)>0", expectedHeader, retArray);
  }

  @Test
  public void testModeWithGroupByLevel() {
    String[] expectedHeader =
        new String[] {
          mode("root.*.*.s1"),
          mode("root.*.*.s2"),
          mode("root.*.*.s3"),
          mode("root.*.*.s4"),
          mode("root.*.*.s5"),
          mode("root.*.*.s6"),
        };
    String[] retArray = new String[] {"2,2,false,2.0,2.0,2,"};
    resultSetEqualTest(
        "select mode(s1),mode(s2),mode(s3),mode(s4),mode(s5),mode(s6) from root.db.* group by level = 0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testModeWithSlidingWindow() {
    assertTestFail(
        "select mode(s1) from root.db.d1 group by time([1,10),3ms,2ms)",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": MODE with slidingWindow is not supported now");
  }

  @Test
  public void testNoMode() {
    String[] expectedHeader = new String[] {mode("root.test.d1.s1")};
    String[] retArray = new String[] {"5.0,"};
    resultSetEqualTest(
        "select mode(s1) from root.test.d1 where time <= 6", expectedHeader, retArray);
  }

  @Test
  public void testManyModes() {
    String[] expectedHeader = new String[] {mode("root.test.d1.s1")};
    String[] retArray = new String[] {"5.0,"};
    resultSetEqualTest("select mode(s1) from root.test.d1", expectedHeader, retArray);
  }
}
