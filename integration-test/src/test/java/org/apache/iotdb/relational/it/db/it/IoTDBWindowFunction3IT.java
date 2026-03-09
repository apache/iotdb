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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBWindowFunction3IT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table demo (device string tag, value double field)",
        "insert into demo values (2021-01-01T09:05:00, 'd1', 3)",
        "insert into demo values (2021-01-01T09:07:00, 'd1', 5)",
        "insert into demo values (2021-01-01T09:09:00, 'd1', 3)",
        "insert into demo values (2021-01-01T09:10:00, 'd1', 1)",
        "insert into demo values (2021-01-01T09:08:00, 'd2', 2)",
        "insert into demo values (2021-01-01T09:15:00, 'd2', 4)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(1024 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMergeWindowFunctions() {
    String[] expectedHeader = new String[] {"time", "device", "value", "a", "b"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,3.0,4.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,5.0,6.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3.0,4.0,",
          "2021-01-01T09:10:00.000Z,d1,1.0,1.0,2.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,2.0,4.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,4.0,6.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, a + min(value) OVER (PARTITION BY device ORDER BY value) as b FROM (SELECT *, max(value) OVER (PARTITION BY device ORDER BY value) as a FROM demo) ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSwapWindowFunctions() {
    String[] expectedHeader = new String[] {"time", "device", "value", "p1", "p2"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1.0,6.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,1.0,5.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,1.0,6.0,",
          "2021-01-01T09:10:00.000Z,d1,1.0,1.0,1.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,2.0,2.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2.0,4.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, min(value) OVER (PARTITION BY device) as p1, sum(value) OVER (PARTITION BY device, value) as p2 FROM demo ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPushDownFilterIntoWindow() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,2,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY device ORDER BY value) as rn FROM demo) WHERE rn <= 2 ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPushDownLimitIntoWindow() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,2,", "2021-01-01T09:07:00.000Z,d1,5.0,4,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY device ORDER BY value) as rn FROM demo) ORDER BY device, time LIMIT 2 ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testReplaceWindowWithRowNumber() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1,",
          "2021-01-01T09:07:00.000Z,d1,5.0,2,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,",
          "2021-01-01T09:10:00.000Z,d1,1.0,4,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, row_number() OVER (PARTITION BY device) AS rn FROM demo ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testRemoveRedundantWindow() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray = new String[] {};
    tableResultSetEqualTest(
        "SELECT *, row_number() OVER (PARTITION BY device) AS rn FROM demo WHERE 1 = 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
