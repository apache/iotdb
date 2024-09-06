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

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCountIfIT {
  // 2 devices 4 regions
  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.db",
        "CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1, 0, 0, true)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(2, null, 0)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(3, 0, 0)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(4, 0, 0)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(5, 1, 0)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(6, 1, 0)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(7, 1, 0)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(8, 0, 0)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(5000000000, 0, 0)",
        "INSERT INTO root.db.d1(timestamp,s1,s2) values(5000000001, 0, 0)",
        "CREATE TIMESERIES root.db.d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.db.d2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.db.d2(timestamp,s1,s2) values(1, 0, 0)",
        "INSERT INTO root.db.d2(timestamp,s1,s2) values(2, null, 0)",
        "INSERT INTO root.db.d2(timestamp,s1,s2) values(5000000000, 0, 0)",
        "INSERT INTO root.db.d2(timestamp,s1,s2) values(5000000001, 0, 0)",
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
  public void testCountIfIgnoreNull() {
    // threshold constant
    String[] expectedHeader =
        new String[] {
          "Count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, 3, \"ignoreNull\"=\"true\")",
          "Count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, 3)"
        };
    String[] retArray = new String[] {"2,1,"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, 3, 'ignoreNull'='true'), Count_if(s1=1 & s2=0, 3) from root.db.d1",
        expectedHeader,
        retArray);

    // keep >= threshold
    expectedHeader =
        new String[] {
          "Count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, keep >= 3, \"ignoreNull\"=\"true\")",
          "Count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, keep >= 3)"
        };
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, keep>=3, 'ignoreNull'='true'), Count_if(s1=1 & s2=0, keep>=3) from root.db.d1",
        expectedHeader,
        retArray);

    // keep < threshold
    expectedHeader =
        new String[] {
          "Count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, keep < 3, \"ignoreNull\"=\"true\")",
          "Count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, keep < 3)"
        };
    retArray = new String[] {"0,0,"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, keep<3, 'ignoreNull'='true'), Count_if(s1=1 & s2=0, keep<3) from root.db.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testCountIfRespectNull() {
    // threshold constant
    String[] expectedHeader =
        new String[] {
          "Count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, 3, \"ignoreNull\"=\"false\")",
          "Count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, 3, \"ignoreNull\"=\"false\")"
        };
    String[] retArray = new String[] {"1,1,"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, 3, 'ignoreNull'='false'), Count_if(s1=1 & s2=0, 3, 'ignoreNull'='false') from root.db.d1",
        expectedHeader,
        retArray);

    // keep >= threshold
    expectedHeader =
        new String[] {
          "Count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, keep >= 3, \"ignoreNull\"=\"false\")",
          "Count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, keep >= 3, \"ignoreNull\"=\"false\")"
        };
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, keep>=3, 'ignoreNull'='false'), Count_if(s1=1 & s2=0, keep>=3, 'ignoreNull'='false') from root.db.d1",
        expectedHeader,
        retArray);

    // keep < threshold
    expectedHeader =
        new String[] {
          "Count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, keep < 3, \"ignoreNull\"=\"false\")",
          "Count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, keep < 3, \"ignoreNull\"=\"false\")"
        };
    retArray = new String[] {"2,0,"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, keep<3, 'ignoreNull'='false'), Count_if(s1=1 & s2=0, keep<3, 'ignoreNull'='false') from root.db.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testCountIfAlignByDevice() {
    String[] expectedHeader =
        new String[] {
          DEVICE,
          "Count_if(s1 = 0 & s2 = 0, 3, \"ignoreNull\"=\"true\")",
          "Count_if(s1 = 1 & s2 = 0, 3)"
        };
    String[] retArray = new String[] {"root.db.d1,2,1,", "root.db.d2,1,0,"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, 3, 'ignoreNull'='true'), Count_if(s1=1 & s2=0, 3) from root.db.* align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testCountIfInHaving() {
    String[] expectedHeader =
        new String[] {
          "Count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, 3, \"ignoreNull\"=\"true\")"
        };
    String[] retArray = new String[] {};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, 3, 'ignoreNull'='true') from root.db.d1 having Count_if(s1=1 & s2=0, 3) > 1",
        expectedHeader,
        retArray);

    retArray = new String[] {"2,"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, 3, 'ignoreNull'='true') from root.db.d1 having Count_if(s1=1 & s2=0, 3) > 0",
        expectedHeader,
        retArray);

    // align by device
    expectedHeader = new String[] {DEVICE, "Count_if(s1 = 0 & s2 = 0, 3, \"ignoreNull\"=\"true\")"};
    retArray = new String[] {};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, 3, 'ignoreNull'='true') from root.db.d1 having Count_if(s1=1 & s2=0, 3) > 1 align by device",
        expectedHeader,
        retArray);

    retArray = new String[] {"root.db.d1,2,"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, 3, 'ignoreNull'='true') from root.db.d1 having Count_if(s1=1 & s2=0, 3) > 0 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testContIfWithoutTransform() {
    String[] expectedHeader = new String[] {"Count_if(root.db.d1.s3, 1)"};
    String[] retArray = new String[] {"1,"};
    resultSetEqualTest("select Count_if(s3, 1) from root.db.d1", expectedHeader, retArray);
  }

  @Test
  public void testContIfWithGroupByLevel() {
    String[] expectedHeader = new String[] {"Count_if(root.db.*.s1 = 0 & root.db.*.s2 = 0, 3)"};
    String[] retArray = new String[] {"4,"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, 3) from root.db.* group by level = 1",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Count_if(root.db.*.s1 = 0 & root.db.*.s2 = 0, keep >= 3)"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, keep>=3) from root.db.* group by level = 1",
        expectedHeader,
        retArray);

    expectedHeader =
        new String[] {
          "Count_if(root.*.d1.s1 = 0 & root.*.d1.s2 = 0, 3)",
          "Count_if(root.*.d1.s1 = 0 & root.*.d2.s2 = 0, 3)",
          "Count_if(root.*.d2.s1 = 0 & root.*.d1.s2 = 0, 3)",
          "Count_if(root.*.d2.s1 = 0 & root.*.d2.s2 = 0, 3)"
        };
    retArray = new String[] {"2,0,1,1,"};
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, 3) from root.db.* group by level = 2",
        expectedHeader,
        retArray);

    expectedHeader =
        new String[] {
          "Count_if(root.*.d1.s1 = 0 & root.*.d1.s2 = 0, keep >= 3)",
          "Count_if(root.*.d1.s1 = 0 & root.*.d2.s2 = 0, keep >= 3)",
          "Count_if(root.*.d2.s1 = 0 & root.*.d1.s2 = 0, keep >= 3)",
          "Count_if(root.*.d2.s1 = 0 & root.*.d2.s2 = 0, keep >= 3)"
        };
    resultSetEqualTest(
        "select Count_if(s1=0 & s2=0, keep>=3) from root.db.* group by level = 2",
        expectedHeader,
        retArray);
  }

  @Test
  public void testContIfWithSlidingWindow() {
    assertTestFail(
        "select count_if(s1>1,1) from root.db.d1 group by time([1,10),3ms,2ms)",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": COUNT_IF with slidingWindow is not supported now");
  }
}
