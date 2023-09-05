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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCastFunctionIT {
  private static final String[] SQLs =
      new String[] {
        // normal cases
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        // data for int series
        "INSERT INTO root.sg.d1(timestamp,s1) values(0, 0)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(1, 1)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(2, 2)",
        "INSERT INTO root.sg.d1(timestamp,s1) values(3, 3)",
        // data for long series
        "INSERT INTO root.sg.d1(timestamp,s2) values(0, 0)",
        "INSERT INTO root.sg.d1(timestamp,s2) values(1, 1)",
        "INSERT INTO root.sg.d1(timestamp,s2) values(2, 2)",
        "INSERT INTO root.sg.d1(timestamp,s2) values(3, 3)",
        // data for float series
        "INSERT INTO root.sg.d1(timestamp,s3) values(0, 0)",
        "INSERT INTO root.sg.d1(timestamp,s3) values(1, 1)",
        "INSERT INTO root.sg.d1(timestamp,s3) values(2, 2.7)",
        "INSERT INTO root.sg.d1(timestamp,s3) values(3, 3.33)",
        // data for double series
        "INSERT INTO root.sg.d1(timestamp,s4) values(0, 0)",
        "INSERT INTO root.sg.d1(timestamp,s4) values(1, 1.0)",
        "INSERT INTO root.sg.d1(timestamp,s4) values(2, 2.7)",
        "INSERT INTO root.sg.d1(timestamp,s4) values(3, 3.33)",
        // data for boolean series
        "INSERT INTO root.sg.d1(timestamp,s5) values(0, false)",
        "INSERT INTO root.sg.d1(timestamp,s5) values(1, false)",
        "INSERT INTO root.sg.d1(timestamp,s5) values(2, true)",
        "INSERT INTO root.sg.d1(timestamp,s5) values(3, true)",
        // data for text series
        "INSERT INTO root.sg.d1(timestamp,s6) values(0, \"10000\")",
        "INSERT INTO root.sg.d1(timestamp,s6) values(1, \"3\")",
        "INSERT INTO root.sg.d1(timestamp,s6) values(2, \"TRue\")",
        "INSERT INTO root.sg.d1(timestamp,s6) values(3, \"faLse\")",
        "flush",

        // special cases
        "create DATABASE root.sg1",
        "create timeseries root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "create timeseries root.sg1.d1.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "create timeseries root.sg1.d1.s3 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "create timeseries root.sg1.d1.s4 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "create timeseries root.sg1.d1.s5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "create timeseries root.sg1.d1.s6 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(timestamp,s2) values(1, 2147483648)",
        "INSERT INTO root.sg1.d1(timestamp,s3) values(1, 2147483648.0)",
        "INSERT INTO root.sg1.d1(timestamp,s3) values(2, 2e38)",
        "INSERT INTO root.sg1.d1(timestamp,s4) values(1, 4e50)",
        "INSERT INTO root.sg1.d1(timestamp,s6) values(1, \"test\")",
        "INSERT INTO root.sg1.d1(timestamp,s6) values(2, \"1.1\")",
        "INSERT INTO root.sg1.d1(timestamp,s6) values(3, \"4e60\")",
        "INSERT INTO root.sg1.d1(timestamp,s6) values(4, \"4e60000\")",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
    registerUDF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function constvalue as 'org.apache.iotdb.db.query.udf.example.ConstValue'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  // region ================== New Transformer ==================
  @Test
  public void testNewTransformerWithIntSource() {
    // cast to int
    String[] intExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s1 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest("select CAST(s1 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s1 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select CAST(s1 AS INT64) from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s1 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.0,",
        };
    resultSetEqualTest(
        "select CAST(s1 AS FLOAT) from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s1 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.0,",
        };
    resultSetEqualTest(
        "select CAST(s1 AS DOUBLE) from root.sg.d1", doubleExpectedHeader, doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s1 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,true,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select CAST(s1 AS BOOLEAN) from root.sg.d1", booleanExpectedHeader, booleanRetArray);

    // cast to text
    String[] textExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s1 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest("select CAST(s1 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithLongSource() {
    // cast to int
    String[] intExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s2 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest("select CAST(s2 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s2 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select CAST(s2 AS INT64) from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s2 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.0,",
        };
    resultSetEqualTest(
        "select CAST(s2 AS FLOAT) from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s2 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.0,",
        };
    resultSetEqualTest(
        "select CAST(s2 AS DOUBLE) from root.sg.d1", doubleExpectedHeader, doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s2 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,true,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select CAST(s2 AS BOOLEAN) from root.sg.d1", booleanExpectedHeader, booleanRetArray);

    // cast to text
    String[] textExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s2 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest("select CAST(s2 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithFloatSource() {
    // cast to int
    String[] intExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s3 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,1,", "2,3,", "3,3,",
        };
    resultSetEqualTest("select CAST(s3 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s3 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,1,", "2,3,", "3,3,",
        };
    resultSetEqualTest(
        "select CAST(s3 AS INT64) from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s3 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.7,", "3,3.33,",
        };
    resultSetEqualTest(
        "select CAST(s3 AS FLOAT) from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s3 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.700000047683716,", "3,3.3299999237060547,",
        };
    resultSetEqualTest(
        "select CAST(s3 AS DOUBLE) from root.sg.d1", doubleExpectedHeader, doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s3 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,true,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select CAST(s3 AS BOOLEAN) from root.sg.d1", booleanExpectedHeader, booleanRetArray);

    // cast to text
    String[] textExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s3 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.7,", "3,3.33,",
        };
    resultSetEqualTest("select CAST(s3 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithDoubleSource() {
    // cast to int
    String[] intExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s4 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,1,", "2,3,", "3,3,",
        };
    resultSetEqualTest("select CAST(s4 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s4 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,1,", "2,3,", "3,3,",
        };
    resultSetEqualTest(
        "select CAST(s4 AS INT64) from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s4 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.7,", "3,3.33,",
        };
    resultSetEqualTest(
        "select CAST(s4 AS FLOAT) from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s4 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.7,", "3,3.33,",
        };
    resultSetEqualTest(
        "select CAST(s4 AS DOUBLE) from root.sg.d1", doubleExpectedHeader, doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s4 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,true,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select CAST(s4 AS BOOLEAN) from root.sg.d1", booleanExpectedHeader, booleanRetArray);

    // cast to text
    String[] textExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s4 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.7,", "3,3.33,",
        };
    resultSetEqualTest("select CAST(s4 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithBooleanSource() {
    // cast to int
    String[] intExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s5 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,0,", "2,1,", "3,1,",
        };
    resultSetEqualTest("select CAST(s5 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s5 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,0,", "2,1,", "3,1,",
        };
    resultSetEqualTest(
        "select CAST(s5 AS INT64) from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s5 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,0.0,", "2,1.0,", "3,1.0,",
        };
    resultSetEqualTest(
        "select CAST(s5 AS FLOAT) from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s5 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,0.0,", "2,1.0,", "3,1.0,",
        };
    resultSetEqualTest(
        "select CAST(s5 AS DOUBLE) from root.sg.d1", doubleExpectedHeader, doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s5 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,false,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select CAST(s5 AS BOOLEAN) from root.sg.d1", booleanExpectedHeader, booleanRetArray);

    // cast to text
    String[] textExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s5 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,false,", "1,false,", "2,true,", "3,true,",
        };
    resultSetEqualTest("select CAST(s5 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithTextSource() {
    // cast to int
    String[] intExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s6 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,10000,", "1,3,",
        };
    resultSetEqualTest(
        "select CAST(s6 AS INT32) from root.sg.d1 where time < 2", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s6 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,10000,", "1,3,",
        };
    resultSetEqualTest(
        "select CAST(s6 AS INT64) from root.sg.d1 where time < 2",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s6 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,10000.0,", "1,3.0,",
        };
    resultSetEqualTest(
        "select CAST(s6 AS FLOAT) from root.sg.d1 where time < 2",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s6 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,10000.0,", "1,3.0,",
        };
    resultSetEqualTest(
        "select CAST(s6 AS DOUBLE) from root.sg.d1 where time < 2",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s6 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "2,true,", "3,false,",
        };
    resultSetEqualTest(
        "select CAST(s6 AS BOOLEAN) from root.sg.d1 where time >= 2",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader = new String[] {TIMESTAMP_STR, "CAST(root.sg.d1.s6 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,10000,", "1,3,", "2,TRue,", "3,faLse,",
        };
    resultSetEqualTest("select CAST(s6 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  // endregion

  // region ================== Old Transformer ==================

  @Test
  public void testOldTransformerWithIntSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s1),CAST(root.sg.d1.s1 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s1),CAST(s1 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s1),CAST(root.sg.d1.s1 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s1),CAST(s1 AS INT64) from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s1),CAST(root.sg.d1.s1 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s1),CAST(s1 AS FLOAT) from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s1),CAST(root.sg.d1.s1 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s1),CAST(s1 AS DOUBLE) from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s1),CAST(root.sg.d1.s1 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,true,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s1),CAST(s1 AS BOOLEAN) from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s1),CAST(root.sg.d1.s1 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s1),CAST(s1 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testOldTransformerWithLongSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s2),CAST(root.sg.d1.s2 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s2),CAST(s2 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s2),CAST(root.sg.d1.s2 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s2),CAST(s2 AS INT64) from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s2),CAST(root.sg.d1.s2 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s2),CAST(s2 AS FLOAT) from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s2),CAST(root.sg.d1.s2 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s2),CAST(s2 AS DOUBLE) from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s2),CAST(root.sg.d1.s2 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,true,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s2),CAST(s2 AS BOOLEAN) from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s2),CAST(root.sg.d1.s2 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s2),CAST(s2 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testOldTransformerWithFloatSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s3),CAST(root.sg.d1.s3 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,3,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s3),CAST(s3 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s3),CAST(root.sg.d1.s3 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,3,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s3),CAST(s3 AS INT64) from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s3),CAST(root.sg.d1.s3 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.7,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s3),CAST(s3 AS FLOAT) from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s3),CAST(root.sg.d1.s3 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.700000047683716,", "3,1,3.3299999237060547,",
        };
    resultSetEqualTest(
        "select constvalue(s3),CAST(s3 AS DOUBLE) from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s3),CAST(root.sg.d1.s3 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,true,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s3),CAST(s3 AS BOOLEAN) from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s3),CAST(root.sg.d1.s3 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.7,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s3),CAST(s3 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testOldTransformerWithDoubleSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s4),CAST(root.sg.d1.s4 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,3,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s4),CAST(s4 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s4),CAST(root.sg.d1.s4 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,3,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s4),CAST(s4 AS INT64) from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s4),CAST(root.sg.d1.s4 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.7,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s4),CAST(s4 AS FLOAT) from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s4),CAST(root.sg.d1.s4 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.7,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s4),CAST(s4 AS DOUBLE) from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s4),CAST(root.sg.d1.s4 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,true,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s4),CAST(s4 AS BOOLEAN) from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s4),CAST(root.sg.d1.s4 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.7,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s4),CAST(s4 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testOldTransformerWithBooleanSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s5),CAST(root.sg.d1.s5 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,0,", "2,1,1,", "3,1,1,",
        };
    resultSetEqualTest(
        "select constvalue(s5),CAST(s5 AS INT32) from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s5),CAST(root.sg.d1.s5 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,0,", "2,1,1,", "3,1,1,",
        };
    resultSetEqualTest(
        "select constvalue(s5),CAST(s5 AS INT64) from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s5),CAST(root.sg.d1.s5 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,0.0,", "2,1,1.0,", "3,1,1.0,",
        };
    resultSetEqualTest(
        "select constvalue(s5),CAST(s5 AS FLOAT) from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s5),CAST(root.sg.d1.s5 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,0.0,", "2,1,1.0,", "3,1,1.0,",
        };
    resultSetEqualTest(
        "select constvalue(s5),CAST(s5 AS DOUBLE) from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s5),CAST(root.sg.d1.s5 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,false,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s5),CAST(s5 AS BOOLEAN) from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s5),CAST(root.sg.d1.s5 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,1,false,", "1,1,false,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s5),CAST(s5 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testOldTransformerWithTextSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s6),CAST(root.sg.d1.s6 AS INT32)"};
    String[] intRetArray =
        new String[] {
          "0,1,10000,", "1,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s6),CAST(s6 AS INT32) from root.sg.d1 where time < 2",
        intExpectedHeader,
        intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s6),CAST(root.sg.d1.s6 AS INT64)"};
    String[] longRetArray =
        new String[] {
          "0,1,10000,", "1,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s6),CAST(s6 AS INT64) from root.sg.d1 where time < 2",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s6),CAST(root.sg.d1.s6 AS FLOAT)"};
    String[] floatRetArray =
        new String[] {
          "0,1,10000.0,", "1,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s6),CAST(s6 AS FLOAT) from root.sg.d1 where time < 2",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s6),CAST(root.sg.d1.s6 AS DOUBLE)"};
    String[] doubleRetArray =
        new String[] {
          "0,1,10000.0,", "1,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s6),CAST(s6 AS DOUBLE) from root.sg.d1 where time < 2",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s6),CAST(root.sg.d1.s6 AS BOOLEAN)"};
    String[] booleanRetArray =
        new String[] {
          "2,1,true,", "3,1,false,",
        };
    resultSetEqualTest(
        "select constvalue(s6),CAST(s6 AS BOOLEAN) from root.sg.d1 where time >= 2",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "constvalue(root.sg.d1.s6),CAST(root.sg.d1.s6 AS TEXT)"};
    String[] textRetArray =
        new String[] {
          "0,1,10000,", "1,1,3,", "2,1,TRue,", "3,1,faLse,",
        };
    resultSetEqualTest(
        "select constvalue(s6),CAST(s6 AS TEXT) from root.sg.d1", textExpectedHeader, textRetArray);
  }

  // endregion

  // region special cases

  @Test
  public void testCastWithLongSource() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select CAST(s2 AS INT32) from root.sg1.d1");
        fail();
      } catch (Exception ignored) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCastWithFloatSource() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select CAST(s3 AS INT32) from root.sg1.d1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s3 AS INT64) from root.sg1.d1");
        fail();
      } catch (Exception ignored) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCastWithDoubleSource() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select CAST(s4 AS INT32) from root.sg1.d1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s4 AS INT64) from root.sg1.d1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s4 AS Float) from root.sg1.d1");
        fail();
      } catch (Exception ignored) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCastWithTextSource() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select CAST(s6 AS INT32) from root.sg1.d1 where time = 1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS INT32) from root.sg1.d1 where time = 2");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS INT64) from root.sg1.d1 where time = 1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS INT64) from root.sg1.d1 where time = 2");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS FLOAT) from root.sg1.d1 where time=3");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS FLOAT) from root.sg1.d1 where time=1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS DOUBLE) from root.sg1.d1 where time = 1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS DOUBLE) from root.sg1.d1 where time = 4");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS BOOLEAN) from root.sg1.d1 where time = 1");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  // endregion

}
