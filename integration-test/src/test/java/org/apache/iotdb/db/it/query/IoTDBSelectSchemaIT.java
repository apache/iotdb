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

package org.apache.iotdb.db.it.query;

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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSelectSchemaIT {
  private static String INSERTION_SQLS =
      "insert into root.sg.d1(time, s1, s2, s3) values (1, 1, 2, 3.0);";

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg");
      statement.execute("CREATE TIMESERIES root.sg.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s2 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s3 with datatype=DOUBLE,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(INSERTION_SQLS);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSchemaExpression() {
    String[] expressions = {
      "s1+s2",
      "-s1+s2",
      "-(s1+s3)",
      "!(s1>s2)",
      "-(-(s1))",
      "((s1+s2)*s3)",
      "-2+s1",
      "!true||s1>0",
      "-(-1)+s1",
      "sin(s1)+s1",
      "((s1+1)*2-1)%2+1.5+s2"
    };
    String[] completeExpressions = {
      "root.sg.d1.s1+root.sg.d1.s2",
      "-root.sg.d1.s1+root.sg.d1.s2",
      "-(root.sg.d1.s1+root.sg.d1.s3)",
      "!(root.sg.d1.s1>root.sg.d1.s2)",
      "-(-root.sg.d1.s1)",
      "(root.sg.d1.s1+root.sg.d1.s2)*root.sg.d1.s3",
      "-2+root.sg.d1.s1",
      "!true|root.sg.d1.s1>0",
      "-(-1)+root.sg.d1.s1",
      "sin(root.sg.d1.s1)+root.sg.d1.s1",
      "((root.sg.d1.s1+1)*2-1)%2+1.5+root.sg.d1.s2",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s from root.sg.d1",
                  expressions[0],
                  expressions[1],
                  expressions[2],
                  expressions[3],
                  expressions[4],
                  expressions[5],
                  expressions[6],
                  expressions[7],
                  expressions[8],
                  expressions[9],
                  expressions[10]));
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + expressions.length, columnCount);

      for (int i = 0; i < expressions.length; ++i) {
        assertEquals(
            completeExpressions[i], resultSet.getMetaData().getColumnName(i + 2).replace(" ", ""));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
