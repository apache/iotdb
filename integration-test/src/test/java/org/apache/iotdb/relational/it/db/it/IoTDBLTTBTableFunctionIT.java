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

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLTTBTableFunctionIT {

  private static final String DATABASE_NAME = "lttb";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE metrics(device STRING TAG, s1 DOUBLE FIELD, s2 DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO metrics(time, device, s1, s2) VALUES "
              + "(0, 'd1', 0.0, 0.0),"
              + "(1, 'd1', 1.0, 0.0),"
              + "(2, 'd1', 8.0, null),"
              + "(3, 'd1', 2.0, 9.0),"
              + "(4, 'd1', 0.0, 1.0),"
              + "(5, 'd1', 0.0, 0.0),"
              + "(0, 'd2', null, null),"
              + "(1, 'd2', null, null)");
      statement.execute("FLUSH");
    } catch (Exception e) {
      fail("insertData failed: " + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTargetCountPreservesEndpointsAndAlignsColumns() {
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(DATA => metrics PARTITION BY device ORDER BY time, "
            + "TIMECOL => 'time', N => 3) ORDER BY device, s1_time",
        new String[] {"window_index", "device", "s1_time", "s1", "s2_time", "s2"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.000Z,0.0,1970-01-01T00:00:00.000Z,0.0,",
          "0,d1,1970-01-01T00:00:00.002Z,8.0,1970-01-01T00:00:00.003Z,9.0,",
          "0,d1,1970-01-01T00:00:00.005Z,0.0,1970-01-01T00:00:00.005Z,0.0,",
          "0,d2,null,null,null,null,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testCountWindowAndTimeWindowOutput() {
    tableResultSetEqualTest(
        "SELECT * FROM LTTB(DATA => (SELECT time, device, s1 FROM metrics WHERE device = 'd1') "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', SIZE => 2) "
            + "ORDER BY window_index",
        new String[] {"window_index", "device", "s1_time", "s1"},
        new String[] {
          "0,d1,1970-01-01T00:00:00.001Z,1.0,",
          "1,d1,1970-01-01T00:00:00.002Z,8.0,",
          "2,d1,1970-01-01T00:00:00.004Z,0.0,"
        },
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT * FROM LTTB(DATA => (SELECT time, device, s1 FROM metrics WHERE device = 'd1') "
            + "PARTITION BY device ORDER BY time, TIMECOL => 'time', SIZE => 2ms, "
            + "ORIGIN => 1970-01-01T00:00:00.000+00:00) ORDER BY window_start",
        new String[] {"window_start", "window_end", "device", "s1_time", "s1"},
        new String[] {
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.002Z,d1,1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,1970-01-01T00:00:00.004Z,d1,1970-01-01T00:00:00.002Z,8.0,",
          "1970-01-01T00:00:00.004Z,1970-01-01T00:00:00.006Z,d1,1970-01-01T00:00:00.004Z,0.0,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testInvalidParameterCombinations() {
    String prefix =
        "SELECT * FROM LTTB(DATA => metrics PARTITION BY device ORDER BY time, TIMECOL => 'time', ";
    tableAssertTestFail(
        "SELECT * FROM LTTB(DATA => metrics PARTITION BY device ORDER BY time, TIMECOL => 'time')",
        "701: Exactly one of the N and SIZE arguments must be specified for LTTB.",
        DATABASE_NAME);
    tableAssertTestFail(
        prefix + "N => 3, SIZE => 2)",
        "701: Exactly one of the N and SIZE arguments must be specified for LTTB.",
        DATABASE_NAME);
    tableAssertTestFail(
        prefix + "N => 2)", "701: The N argument of LTTB must be at least 3.", DATABASE_NAME);
    tableAssertTestFail(
        prefix + "N => 3, SLIDE => 2)",
        "701: The N argument of LTTB cannot be combined with the SLIDE or ORIGIN arguments.",
        DATABASE_NAME);
    tableAssertTestFail(
        prefix + "SIZE => 2, ORIGIN => 1970-01-01T00:00:00.000+00:00)",
        "701: The ORIGIN argument is only supported in time window mode.",
        DATABASE_NAME);
  }
}
