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
public class IoTDBReplaceFunctionIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s2 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s2 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s2 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg.s2 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "INSERT INTO root.sg(timestamp,s1,s2,s3,s4,s5,s6) values(1, 'abcd', 1, 1, 1, 1, true)",
        "INSERT INTO root.sg(timestamp,s1) values(2, 'test\\\\')",
        "INSERT INTO root.sg(timestamp,s1) values(3, 'abcd\\\\')",
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
          TIMESTAMP_STR, "REPLACE(root.sg.s1, 'ab', 'AB')", "REPLACE(root.sg.s1, '\\', 'a')"
        };
    String[] retArray =
        new String[] {
          "1,ABcd,abcd,", "2,test\\\\,testaa,", "3,ABcd\\\\,abcdaa,",
        };
    resultSetEqualTest(
        "select REPLACE(s1, 'ab', 'AB'), REPLACE(s1, '\\', 'a') from root.sg",
        expectedHeader,
        retArray);
  }

  @Test
  public void testOldTransformer() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "constvalue(root.sg.s1)",
          "REPLACE(root.sg.s1, 'ab', 'AB')",
          "REPLACE(root.sg.s1, '\\', 'a')"
        };
    String[] retArray =
        new String[] {
          "1,1,ABcd,abcd,", "2,1,test\\\\,testaa,", "3,1,ABcd\\\\,abcdaa,",
        };
    resultSetEqualTest(
        "select constvalue(s1),REPLACE(s1, 'ab', 'AB'), REPLACE(s1, '\\', 'a') from root.sg",
        expectedHeader,
        retArray);
  }

  @Test
  public void testWithoutFromOrTo() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select REPLACE(s1, 'b') from root.sg");
        fail();
      } catch (Exception ignored) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testWrongInputType() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select REPLACE(s2, 'a', 'b') from root.sg");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s3, 'a', 'b') from root.sg");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s4, 'a', 'b') from root.sg");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s5, 'a', 'b') from root.sg");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s6, 'a', 'b') from root.sg");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }
}
