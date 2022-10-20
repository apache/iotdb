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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
@Ignore
public class IoTDBCQIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
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
        // TODO add assert for error message
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
        // TODO add assert for error message
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
        // TODO add assert for error message
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
        // TODO add assert for error message
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
        // TODO add assert for error message
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
        // TODO add assert for error message
      }

      // 7. start_time_offset == 0
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE RANGE 0m\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(30m)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        // TODO add assert for error message
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
        // TODO add assert for error message
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
        // TODO add assert for error message
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
        // TODO add assert for error message
      }

      // 11. group_by_interval > start_time_offset
      try {
        String sql =
            "CREATE CQ s1_count_cq_2\n"
                + "RESAMPLE RANGE 30m\n"
                + "BEGIN \n"
                + "  SELECT count(s1)  \n"
                + "    INTO root.sg_count.d(count_s1)\n"
                + "    FROM root.sg.d\n"
                + "    GROUP BY(1h)\n"
                + "END";
        statement.execute(sql);
        fail();
      } catch (Exception e) {
        // TODO add assert for error message
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
        // TODO add assert for error message
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
        // TODO add assert for error message
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
          assertEquals(cqIds[cnt], resultSet.getString(0));
          assertEquals(cqSQLs[cnt], resultSet.getString(1));
          assertEquals("ACTIVE", resultSet.getString(2));
          cnt++;
        }
        assertEquals(cqIds.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
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
          assertEquals(cqIds[cnt], resultSet.getString(0));
          assertEquals(cqSQLs[cnt], resultSet.getString(1));
          assertEquals("ACTIVE", resultSet.getString(2));
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
          assertEquals(cqIds[resultIndex[cnt]], resultSet.getString(0));
          assertEquals(cqSQLs[resultIndex[cnt]], resultSet.getString(1));
          assertEquals("ACTIVE", resultSet.getString(2));
          cnt++;
        }
        assertEquals(resultIndex.length, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
