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
        // d1: values 10,10,20,20,20,30 -> change points at rows with values 10,20,30 (first of
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
    // The change-point SQL pattern keeps only the first occurrence of each consecutive run.
    // SELECT * FROM (SELECT *, LAG(measurement) OVER (...) AS prev FROM t) WHERE measurement <>
    // prev OR prev IS NULL
    //
    // d1 values: 10,10,20,20,20,30
    //   row 1 (10): prev=NULL             -> emitted (first row)
    //   row 2 (10): prev=10, 10!=10 false -> not emitted
    //   row 3 (20): prev=10, 20!=10 true  -> emitted (first 20)
    //   row 4 (20): prev=20, 20!=20 false -> not emitted
    //   row 5 (20): prev=20, 20!=20 false -> not emitted
    //   row 6 (30): prev=20, 30!=20 true  -> emitted (first 30)
    //
    // d2 values: 100,200,200,300
    //   row 1 (100): prev=NULL, emitted (first row)
    //   row 2 (200): prev=100, true  -> emitted (first 200)
    //   row 3 (200): prev=200, false -> not emitted
    //   row 4 (300): prev=200, true  -> emitted (first 300)

    String sql =
        "SELECT time, device, measurement, prev FROM "
            + "(SELECT *, LAG(measurement) OVER (PARTITION BY device ORDER BY time) AS prev FROM cp_data) "
            + "WHERE measurement <> prev OR prev IS NULL "
            + "ORDER BY device, time";

    String[] expectedHeader = new String[] {"time", "device", "measurement", "prev"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,10,null,",
          "1970-01-01T00:00:00.003Z,d1,20,10,",
          "1970-01-01T00:00:00.006Z,d1,30,20,",
          "1970-01-01T00:00:00.001Z,d2,100,null,",
          "1970-01-01T00:00:00.002Z,d2,200,100,",
          "1970-01-01T00:00:00.004Z,d2,300,200,",
        };

    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testChangePointAllSameValues() {
    // All values are the same -> only the first row should be emitted (prev IS NULL)
    String sql =
        "SELECT time, device, measurement, prev FROM "
            + "(SELECT *, LAG(measurement) OVER (PARTITION BY device ORDER BY time) AS prev FROM cp_same) "
            + "WHERE measurement <> prev OR prev IS NULL "
            + "ORDER BY device, time";

    String[] expectedHeader = new String[] {"time", "device", "measurement", "prev"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,42,null,",
        };

    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testChangePointAllDifferentValues() {
    // All values are different -> every row should be emitted
    String sql =
        "SELECT time, device, measurement, prev FROM "
            + "(SELECT *, LAG(measurement) OVER (PARTITION BY device ORDER BY time) AS prev FROM cp_diff) "
            + "WHERE measurement <> prev OR prev IS NULL "
            + "ORDER BY device, time";

    String[] expectedHeader = new String[] {"time", "device", "measurement", "prev"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,null,",
          "1970-01-01T00:00:00.002Z,d1,2,1,",
          "1970-01-01T00:00:00.003Z,d1,3,2,",
        };

    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testChangePointSingleRow() {
    // Single row -> always emitted (prev IS NULL)
    String sql =
        "SELECT time, device, measurement, prev FROM "
            + "(SELECT *, LAG(measurement) OVER (PARTITION BY device ORDER BY time) AS prev FROM cp_single) "
            + "WHERE measurement <> prev OR prev IS NULL "
            + "ORDER BY device, time";

    String[] expectedHeader = new String[] {"time", "device", "measurement", "prev"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,99,null,",
        };

    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE_NAME);
  }
}
