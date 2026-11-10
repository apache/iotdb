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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

/**
 * Multi-region coverage for nested set-semantic TVFs and window functions whose PARTITION BY
 * reorders a subset of the child's partition keys. Every device is a single row of {@code t}, so
 * one outer partition spans all DataRegions and any accidental per-region split changes the window
 * count.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBWindowTVFRegionsIT {

  private static final String DATABASE_NAME = "test";
  private static final int DEVICE_NUM = 40;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setDefaultDataRegionGroupNumPerDatabase(4);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE t(a STRING TAG, b STRING TAG, c STRING TAG, s1 DOUBLE FIELD)");
      for (int i = 0; i < DEVICE_NUM; i++) {
        statement.execute(
            String.format(
                "INSERT INTO t(time, a, b, c, s1) VALUES (%d, 'A', 'B', 'c%02d', %d.0)",
                i + 1, i, i + 1));
      }
      statement.execute("FLUSH");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testReorderedSubsetPartitionStaysSingleInstance() {
    // The outer PARTITION BY (b, a) is a strict subset of the inner one in a different order. One
    // outer partition spans every DataRegion, so the outer SESSION must stay single-instance:
    // exactly one window covering all 40 devices. A per-region split would return one window per
    // region, each counting only that region's devices.
    tableResultSetEqualTest(
        "SELECT window_start, window_end, count(*) AS cnt FROM SESSION(DATA => ("
            + "    SELECT window_start AS inner_start, window_end AS inner_end, time, a, b, c, s1 "
            + "    FROM SESSION(DATA => t PARTITION BY (a, b, c) ORDER BY time, GAP => 1ms)"
            + ") PARTITION BY (b, a) ORDER BY time, GAP => 100ms) "
            + "GROUP BY window_start, window_end ORDER BY window_start",
        new String[] {"window_start", "window_end", "cnt"},
        new String[] {"1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.040Z,40,"},
        DATABASE_NAME);
  }

  @Test
  public void testReorderedSubsetWindowPartitionStaysSingleInstance() {
    // Same shape for a plain window function: count(s1) over (b, a) must see rows from every
    // region, so every row gets the full-device count.
    tableResultSetEqualTest(
        "SELECT max(cnt) AS max_cnt, min(cnt) AS min_cnt FROM ("
            + "    SELECT count(s1) OVER (PARTITION BY b, a) AS cnt "
            + "    FROM SESSION(DATA => t PARTITION BY (a, b, c) ORDER BY time, GAP => 1ms))",
        new String[] {"max_cnt", "min_cnt"},
        new String[] {"40,40,"},
        DATABASE_NAME);
  }

  @Test
  public void testPermutedExactPartitionColumnsStayParallel() {
    // The intended case: the outer PARTITION BY lists exactly the inner columns in a different
    // order, so each device is its own outer partition and both levels run per region. 40 devices
    // yield 40 windows of one row each.
    tableResultSetEqualTest(
        "SELECT count(*) AS windows, max(cnt) AS max_cnt, min(cnt) AS min_cnt FROM ("
            + "    SELECT window_start, window_end, count(*) AS cnt FROM SESSION(DATA => ("
            + "        SELECT window_start AS inner_start, window_end AS inner_end, time, a, b, c, s1 "
            + "        FROM SESSION(DATA => t PARTITION BY (a, b, c) ORDER BY time, GAP => 1ms)"
            + "    ) PARTITION BY (c, b, a) ORDER BY time, GAP => 100ms) "
            + "    GROUP BY window_start, window_end)",
        new String[] {"windows", "max_cnt", "min_cnt"},
        new String[] {"40,1,1,"},
        DATABASE_NAME);
  }
}
