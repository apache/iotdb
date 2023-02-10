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
package org.apache.iotdb.db.it.cq;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCQIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // =======================================create cq======================================
  @Test
  public void testCreateWrongCQ() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // 1. specify first parameter of group by time
      try {
        String sql =
            "CREATE CQ s1_count_cq \n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY([0, 10), 30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
                + ": CQ: Specifying time range in GROUP BY TIME clause is prohibited.",
            e.getMessage());
      }

      // 2. specify time filter in where clause
      try {
        String sql =
            "CREATE CQ s1_count_cq \n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    WHERE time >= 0 and time <= 10\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
                + ": CQ: Specifying time filters in the query body is prohibited.",
            e.getMessage());
      }

      // 3. no every clause meanwhile no group by time
      try {
        String sql =
            "CREATE CQ s1_count_cq \n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.SEMANTIC_ERROR.getStatusCode()
                + ": CQ: At least one of the parameters `every_interval` and `group_by_interval` needs to be specified.",
            e.getMessage());
      }

      // 4. no INTO clause
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
                + ": CQ: The query body misses an INTO clause.",
            e.getMessage());
      }

      // 5. EVERY interval is less than continuous_query_min_every_interval_in_ms in
      // iotdb-confignode.properties
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE EVERY 50ms\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
                + ": CQ: Every interval [50] should not be lower than the `continuous_query_minimum_every_interval` [1000] configured.",
            e.getMessage());
      }

      // 6. start_time_offset < 0
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE RANGE -1m\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
                + ": Error occurred while parsing SQL to physical plan: line 2:15 extraneous input '-' expecting DURATION_LITERAL",
            e.getMessage());
      }

      // 7. start_time_offset == 0
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE RANGE 0m\n"
                + "BEGIN \n"
                + "  SELECT count(s1)\n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
                + ": CQ: The start time offset should be greater than 0.",
            e.getMessage());
      }

      // 8. end_time_offset < 0
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE RANGE 30m, -1m\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
                + ": Error occurred while parsing SQL to physical plan: line 2:20 extraneous input '-' expecting DURATION_LITERAL",
            e.getMessage());
      }

      // 9. end_time_offset == start_time_offset
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE RANGE 30m, 30m\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
                + ": CQ: The start time offset should be greater than end time offset.",
            e.getMessage());
      }

      // 10. end_time_offset > start_time_offset
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE RANGE 30m, 31m\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
                + ": CQ: The start time offset should be greater than end time offset.",
            e.getMessage());
      }

      // 11. group_by_interval > start_time_offset
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE RANGE 30m\n"
                + "BEGIN \n"
                + "  SELECT count(s1) \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(1h)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
                + ": CQ: The start time offset should be greater than or equal to every interval.",
            e.getMessage());
      }

      // 12. TIMEOUT POLICY is not BLOCKED or DISCARD
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE RANGE 30m\n"
                + "TIMEOUT POLICY UNKNOWN\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(1h)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.SQL_PARSE_ERROR.getStatusCode()
                + ": Error occurred while parsing SQL to physical plan: line 3:15 mismatched input 'UNKNOWN' expecting {BLOCKED, DISCARD}",
            e.getMessage());
      }

      // 13. create duplicated cq
      try {
        String sql =
            "CREATE CQ s1_count_cq \n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.CQ_AlREADY_EXIST.getStatusCode()
                + ": CQ s1_count_cq has already been created.",
            e.getMessage());
      } finally {
        statement.execute("DROP CQ s1_count_cq;");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateCorrectCQ() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] cqIds = {"correct_cq_1", "correct_cq_2", "correct_cq_3", "s1_count_cq_correct"};
      try {
        String sql =
            "CREATE CQ correct_cq_1 \n"
                + "RESAMPLE \n"
                + "  EVERY 30m\n"
                + "  BOUNDARY 0\n"
                + "  RANGE 30m, 10m\n"
                + "TIMEOUT POLICY BLOCKED\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try {
        String sql =
            "CREATE CQ correct_cq_2\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try {
        String sql =
            "CREATE CQ correct_cq_3\n"
                + "RESAMPLE RANGE 30m, 0m\n"
                + "TIMEOUT POLICY DISCARD\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(10m)\n"
                + "END";
        statement.execute(sql);
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      try {
        String sql =
            "CREATE CQ s1_count_cq_correct\n"
                + "RESAMPLE EVERY 30m \n"
                + "TIMEOUT POLICY DISCARD\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(10m)\n"
                + "END";
        statement.execute(sql);
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      for (String cqId : cqIds) {
        statement.execute(String.format("DROP CQ %s;", cqId));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // =======================================show cq======================================
  @Test
  public void testShowCQ() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] cqIds = {"show_cq_1", "show_cq_2", "show_cq_3", "show_cq_4"};
      String[] cqSQLs = {
        "CREATE CQ show_cq_1 \n"
            + "RESAMPLE \n"
            + "  EVERY 30m\n"
            + "  BOUNDARY 0\n"
            + "  RANGE 30m, 10m\n"
            + "TIMEOUT POLICY BLOCKED\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(30m)\n"
            + "END",
        "CREATE CQ show_cq_2\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(30m)\n"
            + "END",
        "CREATE CQ show_cq_3\n"
            + "RESAMPLE RANGE 30m, 0m\n"
            + "TIMEOUT POLICY DISCARD\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(10m)\n"
            + "END",
        "CREATE CQ show_cq_4\n"
            + "RESAMPLE EVERY 30m \n"
            + "TIMEOUT POLICY DISCARD\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(10m)\n"
            + "END"
      };

      for (String sql : cqSQLs) {
        statement.execute(sql);
      }

      try (ResultSet resultSet = statement.executeQuery("show CQS")) {

        int cnt = 0;
        while (resultSet.next()) {
          // No need to add time column for aggregation query
          assertEquals(cqIds[cnt], resultSet.getString(1));
          assertEquals(cqSQLs[cnt], resultSet.getString(2));
          assertEquals("ACTIVE", resultSet.getString(3));
          cnt++;
        }
        assertEquals(cqIds.length, cnt);
      }

      for (String cqId : cqIds) {
        statement.execute(String.format("DROP CQ %s;", cqId));
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowAuth() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] cqIds = {"show_cq_1", "show_cq_2", "show_cq_3", "show_cq_4"};
      String[] cqSQLs = {
        "CREATE CQ show_cq_1 \n"
            + "RESAMPLE \n"
            + "  EVERY 30m\n"
            + "  BOUNDARY 0\n"
            + "  RANGE 30m, 10m\n"
            + "TIMEOUT POLICY BLOCKED\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(30m)\n"
            + "END",
        "CREATE CQ show_cq_2\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(30m)\n"
            + "END",
        "CREATE CQ show_cq_3\n"
            + "RESAMPLE RANGE 30m, 0m\n"
            + "TIMEOUT POLICY DISCARD\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(10m)\n"
            + "END",
        "CREATE CQ show_cq_4\n"
            + "RESAMPLE EVERY 30m \n"
            + "TIMEOUT POLICY DISCARD\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(10m)\n"
            + "END"
      };

      for (String sql : cqSQLs) {
        statement.execute(sql);
      }

      statement.execute("CREATE USER `zmty` 'zmty'");

      try (Connection connection2 = EnvFactory.getEnv().getConnection("zmty", "zmty");
          Statement statement2 = connection2.createStatement()) {
        try {
          statement2.executeQuery("show CQS");
          fail();
        } catch (Exception e) {
          assertEquals(
              TSStatusCode.NO_PERMISSION.getStatusCode()
                  + ": No permissions for this operation, please add privilege SHOW_CONTINUOUS_QUERIES",
              e.getMessage());
        }

        statement.execute("GRANT USER `zmty` PRIVILEGES SHOW_CONTINUOUS_QUERIES");

        try (ResultSet resultSet = statement2.executeQuery("show CQS")) {

          int cnt = 0;
          while (resultSet.next()) {
            // No need to add time column for aggregation query
            assertEquals(cqIds[cnt], resultSet.getString(1));
            assertEquals(cqSQLs[cnt], resultSet.getString(2));
            assertEquals("ACTIVE", resultSet.getString(3));
            cnt++;
          }
          assertEquals(cqIds.length, cnt);
        }
      }

      for (String cqId : cqIds) {
        statement.execute(String.format("DROP CQ %s;", cqId));
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  // =======================================drop cq======================================
  @Test
  public void testDropCQ() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] cqIds = {"drop_cq_1", "drop_cq_2", "drop_cq_3", "drop_cq_4"};
      String[] cqSQLs = {
        "CREATE CQ drop_cq_1 \n"
            + "RESAMPLE \n"
            + "  EVERY 30m\n"
            + "  BOUNDARY 0\n"
            + "  RANGE 30m, 10m\n"
            + "TIMEOUT POLICY BLOCKED\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(30m)\n"
            + "END",
        "CREATE CQ drop_cq_2\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(30m)\n"
            + "END",
        "CREATE CQ drop_cq_3\n"
            + "RESAMPLE RANGE 30m, 0m\n"
            + "TIMEOUT POLICY DISCARD\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(10m)\n"
            + "END",
        "CREATE CQ drop_cq_4\n"
            + "RESAMPLE EVERY 30m \n"
            + "TIMEOUT POLICY DISCARD\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(10m)\n"
            + "END"
      };

      for (String sql : cqSQLs) {
        statement.execute(sql);
      }

      try (ResultSet resultSet = statement.executeQuery("show CQS")) {

        int cnt = 0;
        while (resultSet.next()) {
          // No need to add time column for aggregation query
          assertEquals(cqIds[cnt], resultSet.getString(1));
          assertEquals(cqSQLs[cnt], resultSet.getString(2));
          assertEquals("ACTIVE", resultSet.getString(3));
          cnt++;
        }
        assertEquals(cqIds.length, cnt);
      }

      statement.execute("DROP CQ drop_cq_2");
      statement.execute("DROP CQ drop_cq_3");

      int[] resultIndex = {0, 3};

      try (ResultSet resultSet = statement.executeQuery("show CQS")) {

        int cnt = 0;
        while (resultSet.next()) {
          // No need to add time column for aggregation query
          assertEquals(cqIds[resultIndex[cnt]], resultSet.getString(1));
          assertEquals(cqSQLs[resultIndex[cnt]], resultSet.getString(2));
          assertEquals("ACTIVE", resultSet.getString(3));
          cnt++;
        }
        assertEquals(resultIndex.length, cnt);
      }

      statement.execute("DROP CQ drop_cq_1");
      statement.execute("DROP CQ drop_cq_4");
      try (ResultSet resultSet = statement.executeQuery("show CQS")) {
        assertFalse(resultSet.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
