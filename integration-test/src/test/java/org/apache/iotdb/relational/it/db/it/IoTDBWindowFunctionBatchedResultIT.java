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
public class IoTDBWindowFunctionBatchedResultIT {
  private static final String DATABASE_NAME = "test";

  private static final String[] SQLS =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE batched_rank (device STRING TAG, value INT32 FIELD)",
        "INSERT INTO batched_rank VALUES (2021-01-01T00:00:01, 'd1', 1)",
        "INSERT INTO batched_rank VALUES (2021-01-01T00:00:02, 'd1', 2)",
        "INSERT INTO batched_rank VALUES (2021-01-01T00:00:03, 'd1', 3)",
        "INSERT INTO batched_rank VALUES (2021-01-01T00:00:04, 'd1', 4)",
        "INSERT INTO batched_rank VALUES (2021-01-01T00:00:05, 'd1', 5)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockLineNumber(2);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRankStateAcrossOutputBatches() {
    String[] expectedHeader = new String[] {"value", "rk"};
    String[] retArray =
        new String[] {
          "1,1,", "2,2,", "3,3,", "4,4,", "5,5,",
        };
    tableResultSetEqualTest(
        "SELECT value, rank() OVER (PARTITION BY device ORDER BY value) AS rk FROM batched_rank ORDER BY value",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : SQLS) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }
}
