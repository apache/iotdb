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

package org.apache.iotdb.relational.it.query.old.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSelectSchemaTableIT {
  private static final String DATABASE_NAME = "test";
  private static String[] SQLS =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE sg(device STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 DOUBLE MEASUREMENT)",
        "insert into sg(time, device, s1, s2, s3) values (1, 'd1', 1, 2, 3.0)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(SQLS);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Ignore // TODO After sin supported
  @Test
  public void testSchemaExpression() {
    String[] expressions = {
      "s1+s2",
      "-s1+s2",
      "-(s1+s3)",
      "not(s1>s2)",
      "-(-(s1))",
      "((s1+s2)*s3)",
      "-2+s1",
      "not true or s1>0",
      "-(-1)+s1",
      "sin(s1)+s1",
      "((s1+1)*2-1)%2+1.5+s2"
    };
    String[] completeExpressions = {
      "s1+s2",
      "-s1+s2",
      "-(s1+s3)",
      "not(s1>s2)",
      "-(-s1)",
      "(s1+s2)*s3",
      "-2+s1",
      "not true or s1>0",
      "-(-1)+s1",
      "sin(s1)+s1",
      "((s1+1)*2-1)%2+1.5+s2",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "select time, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s from sg",
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
