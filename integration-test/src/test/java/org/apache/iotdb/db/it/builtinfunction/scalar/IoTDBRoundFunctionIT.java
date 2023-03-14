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
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBRoundFunctionIT {
  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.db",
        "CREATE DATABASE root.db1",
        "CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Beijing)",
        "CREATE TIMESERIES root.db.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db.d1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db.d1.s4 WITH DATATYPE=FLOAT, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db.d1.s5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN tags(city=Nanjing)",
        "CREATE TIMESERIES root.db.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN tags(city=Nanjing)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(1, 2, 3, 0.11234, 101.143445345,true,null)",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(2, 2, 4, 10.11234, 20.1443465345,true,'sss')",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(3, 2, 555, 120.161234, 20.61437245345,true,'sss')",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(4, 2, 12341234, 101.131234, null,true,'sss')",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(5, 2, 55678, 90.116234, 20.8143454345,true,'sss')",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(6, 2, 12355, null, 60.71443345345,true,'sss')",
        "INSERT INTO root.db.d1(timestamp,s1,s2,s3,s4,s5,s6) values(1678695379764, 2, 12345, 120.511234, 10.143425345,null,'sss')",
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
  public void testRound() {
    String[] intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s1)"};
    String[] intRetArray =
        new String[] {
          "1,2.0,", "2,2.0,", "3,2.0,", "4,2.0,", "5,2.0,", "6,2.0,", "1678695379764,2.0,",
        };
    resultSetEqualTest("select round(s1) from root.**", intExpectedHeader, intRetArray);

    intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s2)"};
    intRetArray =
        new String[] {
          "1,3.0,",
          "2,4.0,",
          "3,555.0,",
          "4,1.2341234E7,",
          "5,55678.0,",
          "6,12355.0,",
          "1678695379764,12345.0,",
        };
    resultSetEqualTest("select round(s2) from root.**", intExpectedHeader, intRetArray);

    intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s3)"};
    intRetArray =
        new String[] {
          "1,0.0,", "2,10.0,", "3,120.0,", "4,101.0,", "5,90.0,", "1678695379764,121.0,",
        };
    resultSetEqualTest("select round(s3) from root.**", intExpectedHeader, intRetArray);

    intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s4)"};
    intRetArray =
        new String[] {
          "1,101.0,", "2,20.0,", "3,21.0,", "5,21.0,", "6,61.0,", "1678695379764,10.0,",
        };
    resultSetEqualTest("select round(s4) from root.**", intExpectedHeader, intRetArray);
  }

  @Test
  public void testRoundWithPlaces() {
    String[] intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s1, 1)"};
    String[] intRetArray =
        new String[] {
          "1,2.0,", "2,2.0,", "3,2.0,", "4,2.0,", "5,2.0,", "6,2.0,", "1678695379764,2.0,",
        };
    resultSetEqualTest("select round(s1,1) from root.**", intExpectedHeader, intRetArray);

    intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s2, 1)"};
    intRetArray =
        new String[] {
          "1,3.0,",
          "2,4.0,",
          "3,555.0,",
          "4,1.2341234E7,",
          "5,55678.0,",
          "6,12355.0,",
          "1678695379764,12345.0,",
        };
    resultSetEqualTest("select round(s2,1) from root.**", intExpectedHeader, intRetArray);

    intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s3, 1)"};
    intRetArray =
        new String[] {
          "1,0.1,", "2,10.1,", "3,120.2,", "4,101.1,", "5,90.1,", "1678695379764,120.5,",
        };
    resultSetEqualTest("select round(s3,1) from root.**", intExpectedHeader, intRetArray);

    intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s4, 1)"};
    intRetArray =
        new String[] {
          "1,101.1,", "2,20.1,", "3,20.6,", "5,20.8,", "6,60.7,", "1678695379764,10.1,",
        };
    resultSetEqualTest("select round(s4,1) from root.**", intExpectedHeader, intRetArray);
  }

  @Test
  public void testRoundWithNegativePlaces() {
    String[] intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s1, -1)"};
    String[] intRetArray =
        new String[] {
          "1,2.0,", "2,2.0,", "3,2.0,", "4,2.0,", "5,2.0,", "6,2.0,", "1678695379764,2.0,",
        };
    resultSetEqualTest("select round(s1,-1) from root.**", intExpectedHeader, intRetArray);

    intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s2, -1)"};
    intRetArray =
        new String[] {
          "1,3.0,",
          "2,4.0,",
          "3,555.0,",
          "4,1.2341234E7,",
          "5,55678.0,",
          "6,12355.0,",
          "1678695379764,12345.0,",
        };
    resultSetEqualTest("select round(s2,-1) from root.**", intExpectedHeader, intRetArray);

    intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s3, -1)"};
    intRetArray =
        new String[] {
          "1,0.0,", "2,10.0,", "3,120.0,", "4,100.0,", "5,90.0,", "1678695379764,120.0,",
        };
    resultSetEqualTest("select round(s3,-1) from root.**", intExpectedHeader, intRetArray);

    intExpectedHeader = new String[] {TIMESTAMP_STR, "round(root.db.d1.s4, -1)"};
    intRetArray =
        new String[] {
          "1,100.0,", "2,20.0,", "3,20.0,", "5,20.0,", "6,60.0,", "1678695379764,10.0,",
        };
    resultSetEqualTest("select round(s4,-1) from root.**", intExpectedHeader, intRetArray);
  }

  @Test
  public void testRoundBooleanAndText() {
    assertTestFail(
        "select round(s5) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Input series of Scalar function [ROUND] only supports numeric data types [INT32, INT64, FLOAT, DOUBLE]");

    assertTestFail(
        "select round(s6) from root.**",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Input series of Scalar function [ROUND] only supports numeric data types [INT32, INT64, FLOAT, DOUBLE]");
  }
}
