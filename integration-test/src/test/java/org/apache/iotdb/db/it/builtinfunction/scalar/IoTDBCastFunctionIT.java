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

  // test without target type

  // test old transformer
  // test int -> Binary

  // test column transformer
  // test int -> Binary

  // test nested

  protected static final String[] SQLs =
      new String[] {
        // positive cases
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
        "INSERT INTO root.sg.d1(timestamp,s3) values(2, 2)",
        "INSERT INTO root.sg.d1(timestamp,s3) values(3, 3.33)",
        // data for double series
        "INSERT INTO root.sg.d1(timestamp,s4) values(0, 0)",
        "INSERT INTO root.sg.d1(timestamp,s4) values(1, 1.0)",
        "INSERT INTO root.sg.d1(timestamp,s4) values(2, 2.0)",
        "INSERT INTO root.sg.d1(timestamp,s4) values(3, 3.33)",
        // data for boolean series
        "INSERT INTO root.sg.d1(timestamp,s5) values(0, false)",
        "INSERT INTO root.sg.d1(timestamp,s5) values(1, false)",
        "INSERT INTO root.sg.d1(timestamp,s5) values(2, true)",
        "INSERT INTO root.sg.d1(timestamp,s5) values(3, true)",
        // data for text series
        "INSERT INTO root.sg.d1(timestamp,s6) values(0, \"10000\")",
        "INSERT INTO root.sg.d1(timestamp,s6) values(1, \"3.33\")",
        "INSERT INTO root.sg.d1(timestamp,s6) values(2, \"true\")",
        "INSERT INTO root.sg.d1(timestamp,s6) values(3, \"false\")",
        "flush",

        // illegal cases
        "create DATABASE root.sg1",
        "create timeseries root.sg1.d1.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(timestamp,s1) values(1, \"test\")",
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
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s1, \"type\"=\"INT32\")"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s1,\"type\"=\"INT32\") from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s1, \"type\"=\"INT64\")"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s1,\"type\"=\"INT64\") from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s1, \"type\"=\"FLOAT\")"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.0,",
        };
    resultSetEqualTest(
        "select cast(s1,\"type\"=\"FLOAT\") from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s1, \"type\"=\"DOUBLE\")"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.0,",
        };
    resultSetEqualTest(
        "select cast(s1,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s1, \"type\"=\"BOOLEAN\")"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,true,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select cast(s1,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s1, \"type\"=\"TEXT\")"};
    String[] textRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s1,\"type\"=\"TEXT\") from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithLongSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s2, \"type\"=\"INT32\")"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s2,\"type\"=\"INT32\") from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s2, \"type\"=\"INT64\")"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s2,\"type\"=\"INT64\") from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s2, \"type\"=\"FLOAT\")"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.0,",
        };
    resultSetEqualTest(
        "select cast(s2,\"type\"=\"FLOAT\") from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s2, \"type\"=\"DOUBLE\")"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.0,",
        };
    resultSetEqualTest(
        "select cast(s2,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s2, \"type\"=\"BOOLEAN\")"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,true,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select cast(s2,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s2, \"type\"=\"TEXT\")"};
    String[] textRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s2,\"type\"=\"TEXT\") from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithFloatSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s3, \"type\"=\"INT32\")"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s3,\"type\"=\"INT32\") from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s3, \"type\"=\"INT64\")"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s3,\"type\"=\"INT64\") from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s3, \"type\"=\"FLOAT\")"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.33,",
        };
    resultSetEqualTest(
        "select cast(s3,\"type\"=\"FLOAT\") from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s3, \"type\"=\"DOUBLE\")"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.3299999237060547,",
        };
    resultSetEqualTest(
        "select cast(s3,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s3, \"type\"=\"BOOLEAN\")"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,true,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select cast(s3,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s3, \"type\"=\"TEXT\")"};
    String[] textRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.33,",
        };
    resultSetEqualTest(
        "select cast(s3,\"type\"=\"TEXT\") from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithDoubleSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s4, \"type\"=\"INT32\")"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s4,\"type\"=\"INT32\") from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s4, \"type\"=\"INT64\")"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,1,", "2,2,", "3,3,",
        };
    resultSetEqualTest(
        "select cast(s4,\"type\"=\"INT64\") from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s4, \"type\"=\"FLOAT\")"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.33,",
        };
    resultSetEqualTest(
        "select cast(s4,\"type\"=\"FLOAT\") from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s4, \"type\"=\"DOUBLE\")"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.33,",
        };
    resultSetEqualTest(
        "select cast(s4,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s4, \"type\"=\"BOOLEAN\")"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,true,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select cast(s4,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s4, \"type\"=\"TEXT\")"};
    String[] textRetArray =
        new String[] {
          "0,0.0,", "1,1.0,", "2,2.0,", "3,3.33,",
        };
    resultSetEqualTest(
        "select cast(s4,\"type\"=\"TEXT\") from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithBooleanSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s5, \"type\"=\"INT32\")"};
    String[] intRetArray =
        new String[] {
          "0,0,", "1,0,", "2,1,", "3,1,",
        };
    resultSetEqualTest(
        "select cast(s5,\"type\"=\"INT32\") from root.sg.d1", intExpectedHeader, intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s5, \"type\"=\"INT64\")"};
    String[] longRetArray =
        new String[] {
          "0,0,", "1,0,", "2,1,", "3,1,",
        };
    resultSetEqualTest(
        "select cast(s5,\"type\"=\"INT64\") from root.sg.d1", longExpectedHeader, longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s5, \"type\"=\"FLOAT\")"};
    String[] floatRetArray =
        new String[] {
          "0,0.0,", "1,0.0,", "2,1.0,", "3,1.0,",
        };
    resultSetEqualTest(
        "select cast(s5,\"type\"=\"FLOAT\") from root.sg.d1", floatExpectedHeader, floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s5, \"type\"=\"DOUBLE\")"};
    String[] doubleRetArray =
        new String[] {
          "0,0.0,", "1,0.0,", "2,1.0,", "3,1.0,",
        };
    resultSetEqualTest(
        "select cast(s5,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s5, \"type\"=\"BOOLEAN\")"};
    String[] booleanRetArray =
        new String[] {
          "0,false,", "1,false,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select cast(s5,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s5, \"type\"=\"TEXT\")"};
    String[] textRetArray =
        new String[] {
          "0,false,", "1,false,", "2,true,", "3,true,",
        };
    resultSetEqualTest(
        "select cast(s5,\"type\"=\"TEXT\") from root.sg.d1", textExpectedHeader, textRetArray);
  }

  @Test
  public void testNewTransformerWithTextSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s6, \"type\"=\"INT32\")"};
    String[] intRetArray =
        new String[] {
          "0,10000,", "1,3,",
        };
    resultSetEqualTest(
        "select cast(s6,\"type\"=\"INT32\") from root.sg.d1 where time < 2",
        intExpectedHeader,
        intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s6, \"type\"=\"INT64\")"};
    String[] longRetArray =
        new String[] {
          "0,10000,", "1,3,",
        };
    resultSetEqualTest(
        "select cast(s6,\"type\"=\"INT64\") from root.sg.d1 where time < 2",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s6, \"type\"=\"FLOAT\")"};
    String[] floatRetArray =
        new String[] {
          "0,10000.0,", "1,3.33,",
        };
    resultSetEqualTest(
        "select cast(s6,\"type\"=\"FLOAT\") from root.sg.d1 where time < 2",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s6, \"type\"=\"DOUBLE\")"};
    String[] doubleRetArray =
        new String[] {
          "0,10000.0,", "1,3.33,",
        };
    resultSetEqualTest(
        "select cast(s6,\"type\"=\"DOUBLE\") from root.sg.d1 where time < 2",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s6, \"type\"=\"BOOLEAN\")"};
    String[] booleanRetArray =
        new String[] {
          "2,true,", "3,false,",
        };
    resultSetEqualTest(
        "select cast(s6,\"type\"=\"BOOLEAN\") from root.sg.d1 where time >= 2",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {TIMESTAMP_STR, "cast(root.sg.d1.s6, \"type\"=\"TEXT\")"};
    String[] textRetArray =
        new String[] {
          "0,10000,", "1,3.33,", "2,true,", "3,false,",
        };
    resultSetEqualTest(
        "select cast(s6,\"type\"=\"TEXT\") from root.sg.d1", textExpectedHeader, textRetArray);
  }

  // endregion

  // region ================== Old Transformer ==================

  @Test
  public void testOldTransformerWithIntSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s1),cast(root.sg.d1.s1, \"type\"=\"INT32\")"
        };
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s1),cast(s1,\"type\"=\"INT32\") from root.sg.d1",
        intExpectedHeader,
        intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s1),cast(root.sg.d1.s1, \"type\"=\"INT64\")"
        };
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s1),cast(s1,\"type\"=\"INT64\") from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s1),cast(root.sg.d1.s1, \"type\"=\"FLOAT\")"
        };
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s1),cast(s1,\"type\"=\"FLOAT\") from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s1),cast(root.sg.d1.s1, \"type\"=\"DOUBLE\")"
        };
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s1),cast(s1,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s1),cast(root.sg.d1.s1, \"type\"=\"BOOLEAN\")"
        };
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,true,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s1),cast(s1,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s1),cast(root.sg.d1.s1, \"type\"=\"TEXT\")"
        };
    String[] textRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s1),cast(s1,\"type\"=\"TEXT\") from root.sg.d1",
        textExpectedHeader,
        textRetArray);
  }

  @Test
  public void testOldTransformerWithLongSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s2),cast(root.sg.d1.s2, \"type\"=\"INT32\")"
        };
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s2),cast(s2,\"type\"=\"INT32\") from root.sg.d1",
        intExpectedHeader,
        intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s2),cast(root.sg.d1.s2, \"type\"=\"INT64\")"
        };
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s2),cast(s2,\"type\"=\"INT64\") from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s2),cast(root.sg.d1.s2, \"type\"=\"FLOAT\")"
        };
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s2),cast(s2,\"type\"=\"FLOAT\") from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s2),cast(root.sg.d1.s2, \"type\"=\"DOUBLE\")"
        };
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.0,",
        };
    resultSetEqualTest(
        "select constvalue(s2),cast(s2,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s2),cast(root.sg.d1.s2, \"type\"=\"BOOLEAN\")"
        };
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,true,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s2),cast(s2,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s2),cast(root.sg.d1.s2, \"type\"=\"TEXT\")"
        };
    String[] textRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s2),cast(s2,\"type\"=\"TEXT\") from root.sg.d1",
        textExpectedHeader,
        textRetArray);
  }

  @Test
  public void testOldTransformerWithFloatSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s3),cast(root.sg.d1.s3, \"type\"=\"INT32\")"
        };
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s3),cast(s3,\"type\"=\"INT32\") from root.sg.d1",
        intExpectedHeader,
        intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s3),cast(root.sg.d1.s3, \"type\"=\"INT64\")"
        };
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s3),cast(s3,\"type\"=\"INT64\") from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s3),cast(root.sg.d1.s3, \"type\"=\"FLOAT\")"
        };
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s3),cast(s3,\"type\"=\"FLOAT\") from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s3),cast(root.sg.d1.s3, \"type\"=\"DOUBLE\")"
        };
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.3299999237060547,",
        };
    resultSetEqualTest(
        "select constvalue(s3),cast(s3,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s3),cast(root.sg.d1.s3, \"type\"=\"BOOLEAN\")"
        };
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,true,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s3),cast(s3,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s3),cast(root.sg.d1.s3, \"type\"=\"TEXT\")"
        };
    String[] textRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s3),cast(s3,\"type\"=\"TEXT\") from root.sg.d1",
        textExpectedHeader,
        textRetArray);
  }

  @Test
  public void testOldTransformerWithDoubleSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s4),cast(root.sg.d1.s4, \"type\"=\"INT32\")"
        };
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s4),cast(s4,\"type\"=\"INT32\") from root.sg.d1",
        intExpectedHeader,
        intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s4),cast(root.sg.d1.s4, \"type\"=\"INT64\")"
        };
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,1,", "2,1,2,", "3,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s4),cast(s4,\"type\"=\"INT64\") from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s4),cast(root.sg.d1.s4, \"type\"=\"FLOAT\")"
        };
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s4),cast(s4,\"type\"=\"FLOAT\") from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s4),cast(root.sg.d1.s4, \"type\"=\"DOUBLE\")"
        };
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s4),cast(s4,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s4),cast(root.sg.d1.s4, \"type\"=\"BOOLEAN\")"
        };
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,true,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s4),cast(s4,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s4),cast(root.sg.d1.s4, \"type\"=\"TEXT\")"
        };
    String[] textRetArray =
        new String[] {
          "0,1,0.0,", "1,1,1.0,", "2,1,2.0,", "3,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s4),cast(s4,\"type\"=\"TEXT\") from root.sg.d1",
        textExpectedHeader,
        textRetArray);
  }

  @Test
  public void testOldTransformerWithBooleanSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s5),cast(root.sg.d1.s5, \"type\"=\"INT32\")"
        };
    String[] intRetArray =
        new String[] {
          "0,1,0,", "1,1,0,", "2,1,1,", "3,1,1,",
        };
    resultSetEqualTest(
        "select constvalue(s5),cast(s5,\"type\"=\"INT32\") from root.sg.d1",
        intExpectedHeader,
        intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s5),cast(root.sg.d1.s5, \"type\"=\"INT64\")"
        };
    String[] longRetArray =
        new String[] {
          "0,1,0,", "1,1,0,", "2,1,1,", "3,1,1,",
        };
    resultSetEqualTest(
        "select constvalue(s5),cast(s5,\"type\"=\"INT64\") from root.sg.d1",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s5),cast(root.sg.d1.s5, \"type\"=\"FLOAT\")"
        };
    String[] floatRetArray =
        new String[] {
          "0,1,0.0,", "1,1,0.0,", "2,1,1.0,", "3,1,1.0,",
        };
    resultSetEqualTest(
        "select constvalue(s5),cast(s5,\"type\"=\"FLOAT\") from root.sg.d1",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s5),cast(root.sg.d1.s5, \"type\"=\"DOUBLE\")"
        };
    String[] doubleRetArray =
        new String[] {
          "0,1,0.0,", "1,1,0.0,", "2,1,1.0,", "3,1,1.0,",
        };
    resultSetEqualTest(
        "select constvalue(s5),cast(s5,\"type\"=\"DOUBLE\") from root.sg.d1",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s5),cast(root.sg.d1.s5, \"type\"=\"BOOLEAN\")"
        };
    String[] booleanRetArray =
        new String[] {
          "0,1,false,", "1,1,false,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s5),cast(s5,\"type\"=\"BOOLEAN\") from root.sg.d1",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s5),cast(root.sg.d1.s5, \"type\"=\"TEXT\")"
        };
    String[] textRetArray =
        new String[] {
          "0,1,false,", "1,1,false,", "2,1,true,", "3,1,true,",
        };
    resultSetEqualTest(
        "select constvalue(s5),cast(s5,\"type\"=\"TEXT\") from root.sg.d1",
        textExpectedHeader,
        textRetArray);
  }

  @Test
  public void testOldTransformerWithTextSource() {
    // cast to int
    String[] intExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s6),cast(root.sg.d1.s6, \"type\"=\"INT32\")"
        };
    String[] intRetArray =
        new String[] {
          "0,1,10000,", "1,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s6),cast(s6,\"type\"=\"INT32\") from root.sg.d1 where time < 2",
        intExpectedHeader,
        intRetArray);

    // cast to long
    String[] longExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s6),cast(root.sg.d1.s6, \"type\"=\"INT64\")"
        };
    String[] longRetArray =
        new String[] {
          "0,1,10000,", "1,1,3,",
        };
    resultSetEqualTest(
        "select constvalue(s6),cast(s6,\"type\"=\"INT64\") from root.sg.d1 where time < 2",
        longExpectedHeader,
        longRetArray);

    // cast to float
    String[] floatExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s6),cast(root.sg.d1.s6, \"type\"=\"FLOAT\")"
        };
    String[] floatRetArray =
        new String[] {
          "0,1,10000.0,", "1,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s6),cast(s6,\"type\"=\"FLOAT\") from root.sg.d1 where time < 2",
        floatExpectedHeader,
        floatRetArray);

    // cast to double
    String[] doubleExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s6),cast(root.sg.d1.s6, \"type\"=\"DOUBLE\")"
        };
    String[] doubleRetArray =
        new String[] {
          "0,1,10000.0,", "1,1,3.33,",
        };
    resultSetEqualTest(
        "select constvalue(s6),cast(s6,\"type\"=\"DOUBLE\") from root.sg.d1 where time < 2",
        doubleExpectedHeader,
        doubleRetArray);

    // cast to boolean
    String[] booleanExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s6),cast(root.sg.d1.s6, \"type\"=\"BOOLEAN\")"
        };
    String[] booleanRetArray =
        new String[] {
          "2,1,true,", "3,1,false,",
        };
    resultSetEqualTest(
        "select constvalue(s6),cast(s6,\"type\"=\"BOOLEAN\") from root.sg.d1 where time >= 2",
        booleanExpectedHeader,
        booleanRetArray);

    // cast to text
    String[] textExpectedHeader =
        new String[] {
          TIMESTAMP_STR, "constvalue(root.sg.d1.s6),cast(root.sg.d1.s6, \"type\"=\"TEXT\")"
        };
    String[] textRetArray =
        new String[] {
          "0,1,10000,", "1,1,3.33,", "2,1,true,", "3,1,false,",
        };
    resultSetEqualTest(
        "select constvalue(s6),cast(s6,\"type\"=\"TEXT\") from root.sg.d1",
        textExpectedHeader,
        textRetArray);
  }

  // endregion

  // region illegal cases

  @Test
  public void testCastWithTextSource() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select cast(s1,\"type\"=\"INT32\") from root.sg1.d1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select cast(s1,\"type\"=\"INT64\") from root.sg1.d1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select cast(s1,\"type\"=\"FLOAT\") from root.sg1.d1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select cast(s1,\"type\"=\"DOUBLE\") from root.sg1.d1");
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
