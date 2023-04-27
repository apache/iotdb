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

package org.apache.iotdb.db.it.builtinfunction.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DEVICE;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDiffFunctionIT {
  // 2 devices 4 regions
  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.db",
        "CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s2 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.db.d1(timestamp,s1) values(2, 2)",
        "INSERT INTO root.db.d1(timestamp,s2) values(3, 3)",
        "INSERT INTO root.db.d1(timestamp,s1) values(4, 4)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(5, 5, 5)",
        "INSERT INTO root.db.d1(timestamp,s2) values(6, 6)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(5000000000, null, 7)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(5000000001, 8, null)",
        "CREATE TIMESERIES root.db.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s2 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "INSERT INTO root.db.d2(timestamp,s1,s2) values(1, 1, 1)",
        "INSERT INTO root.db.d2(timestamp,s1,s2) values(2, 2, 2)",
        "INSERT INTO root.db.d2(timestamp,s1,s2) values(5000000000, null, 3)",
        "INSERT INTO root.db.d2(timestamp,s1,s2) values(5000000001, 4, null)",
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
  public void testNewTransformerIgnoreNull() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, "Diff(root.db.d1.s1)", "diff(root.db.d1.s2)"};
    String[] retArray =
        new String[] {
          "1,null,null,",
          "2,1.0,null,",
          "3,null,2.0,",
          "4,2.0,null,",
          "5,1.0,2.0,",
          "6,null,1.0,",
          "5000000000,null,1.0,",
          "5000000001,3.0,null,"
        };
    resultSetEqualTest("select Diff(s1), diff(s2) from root.db.d1", expectedHeader, retArray);

    // align by device
    expectedHeader = new String[] {TIMESTAMP_STR, DEVICE, "Diff(s1)", "diff(s2)"};
    retArray =
        new String[] {
          "1,root.db.d1,null,null,",
          "2,root.db.d1,1.0,null,",
          "3,root.db.d1,null,2.0,",
          "4,root.db.d1,2.0,null,",
          "5,root.db.d1,1.0,2.0,",
          "6,root.db.d1,null,1.0,",
          "5000000000,root.db.d1,null,1.0,",
          "5000000001,root.db.d1,3.0,null,",
          "1,root.db.d2,null,null,",
          "2,root.db.d2,1.0,1.0,",
          "5000000000,root.db.d2,null,1.0,",
          "5000000001,root.db.d2,2.0,null,"
        };
    resultSetEqualTest(
        "select Diff(s1), diff(s2) from root.db.d1, root.db.d2 align by device",
        expectedHeader,
        retArray);

    // align by device + order by time
    retArray =
        new String[] {
          "1,root.db.d1,null,null,",
          "1,root.db.d2,null,null,",
          "2,root.db.d1,1.0,null,",
          "2,root.db.d2,1.0,1.0,",
          "3,root.db.d1,null,2.0,",
          "4,root.db.d1,2.0,null,",
          "5,root.db.d1,1.0,2.0,",
          "6,root.db.d1,null,1.0,",
          "5000000000,root.db.d1,null,1.0,",
          "5000000000,root.db.d2,null,1.0,",
          "5000000001,root.db.d1,3.0,null,",
          "5000000001,root.db.d2,2.0,null,"
        };
    resultSetEqualTest(
        "select Diff(s1), diff(s2) from root.db.d1, root.db.d2 order by time align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testNewTransformerRespectNull() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "Diff(root.db.d1.s1, \"ignoreNull\"=\"false\")",
          "diff(root.db.d1.s2, \"ignoreNull\"=\"false\")"
        };
    String[] retArray =
        new String[] {
          "1,null,null,",
          "2,1.0,null,",
          "3,null,null,",
          "4,null,null,",
          "5,1.0,null,",
          "6,null,1.0,",
          "5000000000,null,1.0,",
          "5000000001,null,null,"
        };
    resultSetEqualTest(
        "select Diff(s1, 'ignoreNull'='false'), diff(s2, 'ignoreNull'='false') from root.db.d1",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          DEVICE,
          "Diff(s1, \"ignoreNull\"=\"false\")",
          "diff(s2, \"ignoreNull\"=\"false\")"
        };
    retArray =
        new String[] {
          "1,root.db.d1,null,null,",
          "2,root.db.d1,1.0,null,",
          "3,root.db.d1,null,null,",
          "4,root.db.d1,null,null,",
          "5,root.db.d1,1.0,null,",
          "6,root.db.d1,null,1.0,",
          "5000000000,root.db.d1,null,1.0,",
          "5000000001,root.db.d1,null,null,",
          "1,root.db.d2,null,null,",
          "2,root.db.d2,1.0,1.0,",
          "5000000000,root.db.d2,null,1.0,",
          "5000000001,root.db.d2,null,null,",
        };
    resultSetEqualTest(
        "select Diff(s1, 'ignoreNull'='false'), diff(s2, 'ignoreNull'='false') from root.db.d1, root.db.d2 align by device",
        expectedHeader,
        retArray);

    // align by device + order by time
    retArray =
        new String[] {
          "1,root.db.d1,null,null,",
          "1,root.db.d2,null,null,",
          "2,root.db.d1,1.0,null,",
          "2,root.db.d2,1.0,1.0,",
          "3,root.db.d1,null,null,",
          "4,root.db.d1,null,null,",
          "5,root.db.d1,1.0,null,",
          "6,root.db.d1,null,1.0,",
          "5000000000,root.db.d1,null,1.0,",
          "5000000000,root.db.d2,null,1.0,",
          "5000000001,root.db.d1,null,null,",
          "5000000001,root.db.d2,null,null,",
        };
    resultSetEqualTest(
        "select Diff(s1, 'ignoreNull'='false'), diff(s2, 'ignoreNull'='false') from root.db.d1, root.db.d2 order by time align by device",
        expectedHeader,
        retArray);
  }

  // [change_points] is not mappable function, so this calculation use old transformer
  @Test
  public void testOldTransformerIgnoreNull() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, "change_points(root.db.d1.s1)", "diff(root.db.d1.s2)"};
    String[] retArray =
        new String[] {
          "1,1,null,",
          "2,2,null,",
          "3,null,2.0,",
          "4,4,null,",
          "5,5,2.0,",
          "6,null,1.0,",
          "5000000000,null,1.0,",
          "5000000001,8,null,"
        };
    resultSetEqualTest(
        "select change_points(s1), diff(s2) from root.db.d1", expectedHeader, retArray);

    // align by device
    expectedHeader = new String[] {TIMESTAMP_STR, DEVICE, "change_points(s1)", "diff(s2)"};
    retArray =
        new String[] {
          "1,root.db.d1,1,null,",
          "2,root.db.d1,2,null,",
          "3,root.db.d1,null,2.0,",
          "4,root.db.d1,4,null,",
          "5,root.db.d1,5,2.0,",
          "6,root.db.d1,null,1.0,",
          "5000000000,root.db.d1,null,1.0,",
          "5000000001,root.db.d1,8,null,",
          "1,root.db.d2,1,null,",
          "2,root.db.d2,2,1.0,",
          "5000000000,root.db.d2,null,1.0,",
          "5000000001,root.db.d2,4,null,",
        };
    resultSetEqualTest(
        "select change_points(s1), diff(s2) from root.db.d1, root.db.d2 align by device",
        expectedHeader,
        retArray);

    // align by device + order by time
    expectedHeader = new String[] {TIMESTAMP_STR, DEVICE, "change_points(s1)", "diff(s2)"};
    retArray =
        new String[] {
          "1,root.db.d1,1,null,",
          "1,root.db.d2,1,null,",
          "2,root.db.d1,2,null,",
          "2,root.db.d2,2,1.0,",
          "3,root.db.d1,null,2.0,",
          "4,root.db.d1,4,null,",
          "5,root.db.d1,5,2.0,",
          "6,root.db.d1,null,1.0,",
          "5000000000,root.db.d1,null,1.0,",
          "5000000000,root.db.d2,null,1.0,",
          "5000000001,root.db.d1,8,null,",
          "5000000001,root.db.d2,4,null,",
        };
    resultSetEqualTest(
        "select change_points(s1), diff(s2) from root.db.d1, root.db.d2 order by time align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testOldTransformerRespectNull() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "change_points(root.db.d1.s1)",
          "diff(root.db.d1.s2, \"ignoreNull\"=\"false\")"
        };
    String[] retArray =
        new String[] {
          "1,1,null,",
          "2,2,null,",
          "4,4,null,",
          "5,5,null,",
          "6,null,1.0,",
          "5000000000,null,1.0,",
          "5000000001,8,null,"
        };
    resultSetEqualTest(
        "select change_points(s1), diff(s2, 'ignoreNull'='false') from root.db.d1",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, DEVICE, "change_points(s1)", "diff(s2, \"ignoreNull\"=\"false\")"
        };
    retArray =
        new String[] {
          "1,root.db.d1,1,null,",
          "2,root.db.d1,2,null,",
          "4,root.db.d1,4,null,",
          "5,root.db.d1,5,null,",
          "6,root.db.d1,null,1.0,",
          "5000000000,root.db.d1,null,1.0,",
          "5000000001,root.db.d1,8,null,",
          "1,root.db.d2,1,null,",
          "2,root.db.d2,2,1.0,",
          "5000000000,root.db.d2,null,1.0,",
          "5000000001,root.db.d2,4,null,",
        };
    resultSetEqualTest(
        "select change_points(s1), diff(s2, 'ignoreNull'='false') from root.db.d1, root.db.d2 align by device",
        expectedHeader,
        retArray);

    // align by device + order by time
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, DEVICE, "change_points(s1)", "diff(s2, \"ignoreNull\"=\"false\")"
        };
    retArray =
        new String[] {
          "1,root.db.d1,1,null,",
          "1,root.db.d2,1,null,",
          "2,root.db.d1,2,null,",
          "2,root.db.d2,2,1.0,",
          "4,root.db.d1,4,null,",
          "5,root.db.d1,5,null,",
          "6,root.db.d1,null,1.0,",
          "5000000000,root.db.d1,null,1.0,",
          "5000000000,root.db.d2,null,1.0,",
          "5000000001,root.db.d1,8,null,",
          "5000000001,root.db.d2,4,null,",
        };
    resultSetEqualTest(
        "select change_points(s1), diff(s2, 'ignoreNull'='false') from root.db.d1, root.db.d2 order by time align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testCaseInSensitive() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "root.db.d1.s1", "root.db.d1.s2"};
    String[] retArray = new String[] {"2,2,null,", "4,4,null,", "5,5,5.0,"};
    resultSetEqualTest(
        "select s1, s2 from root.db.d1 where Diff(s1) between 1 and 2", expectedHeader, retArray);

    retArray = new String[] {};
    resultSetEqualTest(
        "select s1, s2 from root.db.d1 where Diff(notExist) between 1 and 2",
        expectedHeader,
        retArray);
  }
}
