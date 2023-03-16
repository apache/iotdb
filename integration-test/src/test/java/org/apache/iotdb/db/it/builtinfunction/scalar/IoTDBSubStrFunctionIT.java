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
public class IoTDBSubStrFunctionIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s3 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s4 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s5 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s6 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "INSERT INTO root.sg(timestamp,s1,s2,s3,s4,s5,s6) values(1, 'abcd', 1, 1, 1, 1, true)",
        "INSERT INTO root.sg(timestamp,s1) values(2, 'test')",
        "INSERT INTO root.sg(timestamp,s1) values(3, 'abcdefg')",
        "flush"
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

  @Test
  public void testNewTransformer() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "root.sg.s1", "SUBSTR(root.sg.s1,1)", "SUBSTR(root.sg.s1,1,3)"
        };
    String[] retArray =
        new String[] {
          "1,abcd,bcd,bc,", "2,test,est,es,", "3,abcdefg,bcdefg,bc,",
        };
    resultSetEqualTest(
        "select s1,SUBSTR(s1,1),SUBSTR(s1,1,3) from root.sg", expectedHeader, retArray);
  }

  @Test
  public void testOldTransformer() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "root.sg.s1",
          "SUBSTR(root.sg.s1,1)",
          "SUBSTR(root.sg.s1,1,3)",
          "constvalue(root.sg.s1)"
        };
    String[] retArray =
        new String[] {"1,abcd,bcd,bc,1,", "2,test,est,es,1,", "3,abcdefg,bcdefg,bc,1,"};
    resultSetEqualTest(
        "select s1,SUBSTR(s1,1),SUBSTR(s1,1,3),constvalue(s1) from root.sg",
        expectedHeader,
        retArray);
  }

  @Test
  public void testWrongInputType() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select SUBSTR(s2,1) from root.sg");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select SUBSTR(s3,2) from root.sg");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select SUBSTR(s4,1) from root.sg");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select SUBSTR(s5,2) from root.sg");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select SUBSTR(s6,2) from root.sg");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testOutOfLength() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select SUBSTR(s1,111) from root.sg");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select SUBSTR(s1,111,2222) from root.sg");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }
}
