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
public class IoTDBChangePointIT {
  private static final String DATABASE_NAME = "test";

  private static final String[] setupSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        // Table with consecutive duplicate values for change-point detection
        "CREATE TABLE cp_data (device STRING TAG, measurement INT64 FIELD)",
        // d1: values 10,10,20,20,20,30 -> change points at rows with values 10,20,30 (last of
        // each run)
        "INSERT INTO cp_data VALUES (1, 'd1', 10)",
        "INSERT INTO cp_data VALUES (2, 'd1', 10)",
        "INSERT INTO cp_data VALUES (3, 'd1', 20)",
        "INSERT INTO cp_data VALUES (4, 'd1', 20)",
        "INSERT INTO cp_data VALUES (5, 'd1', 20)",
        "INSERT INTO cp_data VALUES (6, 'd1', 30)",
        // d2: values 100,200,200,300 -> change points at rows with values 100,200,300
        "INSERT INTO cp_data VALUES (1, 'd2', 100)",
        "INSERT INTO cp_data VALUES (2, 'd2', 200)",
        "INSERT INTO cp_data VALUES (3, 'd2', 200)",
        "INSERT INTO cp_data VALUES (4, 'd2', 300)",
        // Table for all-same-values test
        "CREATE TABLE cp_same (device STRING TAG, measurement INT64 FIELD)",
        "INSERT INTO cp_same VALUES (1, 'd1', 42)",
        "INSERT INTO cp_same VALUES (2, 'd1', 42)",
        "INSERT INTO cp_same VALUES (3, 'd1', 42)",
        // Table for all-different-values test
        "CREATE TABLE cp_diff (device STRING TAG, measurement INT64 FIELD)",
        "INSERT INTO cp_diff VALUES (1, 'd1', 1)",
        "INSERT INTO cp_diff VALUES (2, 'd1', 2)",
        "INSERT INTO cp_diff VALUES (3, 'd1', 3)",
        // Table for single row test
        "CREATE TABLE cp_single (device STRING TAG, measurement INT64 FIELD)",
        "INSERT INTO cp_single VALUES (1, 'd1', 99)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(1024 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : setupSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("setUp failed: " + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testChangePointDetection() {
    // The change-point SQL pattern:
    // SELECT * FROM (SELECT *, LEAD(measurement) OVER (...) AS next FROM t) WHERE measurement <>
    // next OR next IS NULL
    // This detects the last row of each consecutive run of identical values.
    //
    // d1 values: 10,10,20,20,20,30
    //   row 1 (10): next=10, 10!=10 false -> not emitted
    //   row 2 (10): next=20, 10!=20 true  -> emitted (last 10 before 20)
    //   row 3 (20): next=20, 20!=20 false -> not emitted
    //   row 4 (20): next=20, 20!=20 false -> not emitted
    //   row 5 (20): next=30, 20!=30 true  -> emitted (last 20 before 30)
    //   row 6 (30): next=NULL             -> emitted (last row)
    //
    // d2 values: 100,200,200,300
    //   row 1 (100): next=200, true  -> emitted
    //   row 2 (200): next=200, false -> not emitted
    //   row 3 (200): next=300, true  -> emitted
    //   row 4 (300): next=NULL       -> emitted

    String sql =
        "SELECT time, device, measurement, next FROM "
            + "(SELECT *, LEAD(measurement) OVER (PARTITION BY device ORDER BY time) AS next FROM cp_data) "
            + "WHERE measurement <> next OR next IS NULL "
            + "ORDER BY device, time";

    String[] expectedHeader = new String[] {"time", "device", "measurement", "next"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d1,10,20,",
          "1970-01-01T00:00:00.005Z,d1,20,30,",
          "1970-01-01T00:00:00.006Z,d1,30,null,",
          "1970-01-01T00:00:00.001Z,d2,100,200,",
          "1970-01-01T00:00:00.003Z,d2,200,300,",
          "1970-01-01T00:00:00.004Z,d2,300,null,",
        };

    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testChangePointAllSameValues() {
    // All values are the same -> only the last row should be emitted (next IS NULL)
    String sql =
        "SELECT time, device, measurement, next FROM "
            + "(SELECT *, LEAD(measurement) OVER (PARTITION BY device ORDER BY time) AS next FROM cp_same) "
            + "WHERE measurement <> next OR next IS NULL "
            + "ORDER BY device, time";

    String[] expectedHeader = new String[] {"time", "device", "measurement", "next"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,d1,42,null,",
        };

    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testChangePointAllDifferentValues() {
    // All values are different -> every row should be emitted
    String sql =
        "SELECT time, device, measurement, next FROM "
            + "(SELECT *, LEAD(measurement) OVER (PARTITION BY device ORDER BY time) AS next FROM cp_diff) "
            + "WHERE measurement <> next OR next IS NULL "
            + "ORDER BY device, time";

    String[] expectedHeader = new String[] {"time", "device", "measurement", "next"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,2,",
          "1970-01-01T00:00:00.002Z,d1,2,3,",
          "1970-01-01T00:00:00.003Z,d1,3,null,",
        };

    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testChangePointSingleRow() {
    // Single row -> always emitted (next IS NULL)
    String sql =
        "SELECT time, device, measurement, next FROM "
            + "(SELECT *, LEAD(measurement) OVER (PARTITION BY device ORDER BY time) AS next FROM cp_single) "
            + "WHERE measurement <> next OR next IS NULL "
            + "ORDER BY device, time";

    String[] expectedHeader = new String[] {"time", "device", "measurement", "next"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,99,null,",
        };

    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }
}
