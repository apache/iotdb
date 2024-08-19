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

package org.apache.iotdb.relational.it.query.old.builtinfunction.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDiffFunctionTableIT {

  private static final String DATABASE_NAME = "db";

  // 2 devices 4 regions
  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "use " + DATABASE_NAME,
        "create table table1(device_id STRING ID, s1 INT32 MEASUREMENT, s2 FLOAT MEASUREMENT)",
        "INSERT INTO table1(time,device_id,s1,s2) values(1, 'd1', 1, 1)",
        "INSERT INTO table1(time,device_id,s1) values(2, 'd1', 2)",
        "INSERT INTO table1(time,device_id,s2) values(3, 'd1', 3)",
        "INSERT INTO table1(time,device_id,s1) values(4, 'd1', 4)",
        "INSERT INTO table1(time,device_id,s1,s2) values(5, 'd1', 5, 5)",
        "INSERT INTO table1(time,device_id,s2) values(6, 'd1', 6)",
        "INSERT INTO table1(time,device_id,s1,s2) values(5000000000, 'd1', null, 7)",
        "INSERT INTO table1(time,device_id,s1,s2) values(5000000001, 'd1', 8, null)",
        "INSERT INTO table1(time,device_id,s1,s2) values(1, 'd2', 1, 1)",
        "INSERT INTO table1(time,device_id,s1,s2) values(2, 'd2', 2, 2)",
        "INSERT INTO table1(time,device_id,s1,s2) values(5000000000, 'd2', null, 3)",
        "INSERT INTO table1(time,device_id,s1,s2) values(5000000001, 'd2', 4, null)",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNewTransformerIgnoreNull() {
    String[] expectedHeader = new String[] {"time", "_col1", "_col2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,null,null,",
          "1970-01-01T00:00:00.002Z,1.0,null,",
          "1970-01-01T00:00:00.003Z,null,2.0,",
          "1970-01-01T00:00:00.004Z,2.0,null,",
          "1970-01-01T00:00:00.005Z,1.0,2.0,",
          "1970-01-01T00:00:00.006Z,null,1.0,",
          "1970-02-27T20:53:20.000Z,null,1.0,",
          "1970-02-27T20:53:20.001Z,3.0,null,"
        };
    tableResultSetEqualTest(
        "select time,diff(s1), diff(s2) from table1 where device_id='d1'",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerRespectNull() {
    String[] expectedHeader = new String[] {"time", "_col1", "_col2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,null,null,",
          "1970-01-01T00:00:00.002Z,1.0,null,",
          "1970-01-01T00:00:00.003Z,null,null,",
          "1970-01-01T00:00:00.004Z,null,null,",
          "1970-01-01T00:00:00.005Z,1.0,null,",
          "1970-01-01T00:00:00.006Z,null,1.0,",
          "1970-02-27T20:53:20.000Z,null,1.0,",
          "1970-02-27T20:53:20.001Z,null,null,"
        };
    tableResultSetEqualTest(
        "select time, Diff(s1, false), diff(s2, false) from table1 where device_id='d1'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // align by device + order by time
  }

  @Test
  public void testCaseInSensitive() {
    String[] expectedHeader = new String[] {"time", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,2,null,",
          "1970-01-01T00:00:00.004Z,4,null,",
          "1970-01-01T00:00:00.005Z,5,5.0,"
        };
    tableResultSetEqualTest(
        "select time, s1, s2 from table1 where device_id='d1' and diff(s1) between 1 and 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
