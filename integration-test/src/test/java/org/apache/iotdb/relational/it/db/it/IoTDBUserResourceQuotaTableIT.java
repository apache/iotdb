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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBUserResourceQuotaTableIT {

  private static final String USER_PASSWORD = "IoTDB@2021abc";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setQuotaEnable(true);
    // Small sort buffer forces external-sort spill for read_temp_disk_*. sortBuffer > maxTsBlock.
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024L);
    EnvFactory.getEnv().getConfig().getCommonConfig().setDegreeOfParallelism(4);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setQuotaEnable(false);
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /** SET / SHOW / DELETE USER QUOTA under TABLE dialect: columns, types, and NodeID. */
  @Test
  public void setAndShowUserQuota() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_user '" + USER_PASSWORD + "'");
      statement.execute(
          "SET USER QUOTA ON urq_user WITH read_cpu_min=1, read_cpu_max=4, write_memory_max=10485760, write_disk_io_max=10485760, read_temp_disk_max=8589934592");
      Map<String, String> maxByType = new HashMap<>();
      Set<String> types = new HashSet<>();
      try (ResultSet rs = statement.executeQuery("SHOW USER QUOTA urq_user")) {
        ResultSetMetaData meta = rs.getMetaData();
        int userIdx = columnIndex(meta, "User");
        int nodeIdx = columnIndex(meta, "NodeID");
        int rwIdx = columnIndex(meta, "Read/Write");
        int typeIdx = columnIndex(meta, "QuotaType");
        int minIdx = columnIndex(meta, "Min");
        int maxIdx = columnIndex(meta, "Max");
        int usedIdx = columnIndex(meta, "Used");
        Assert.assertTrue(columnIndex(meta, "MinGap") > 0);
        while (rs.next()) {
          Assert.assertEquals("urq_user", rs.getString(userIdx));
          String nodeId = rs.getString(nodeIdx);
          Assert.assertNotNull(nodeId);
          Assert.assertFalse("-".equals(nodeId));
          Assert.assertTrue(Integer.parseInt(nodeId) > 0);
          String key = rs.getString(rwIdx) + ":" + rs.getString(typeIdx);
          maxByType.put(key, rs.getString(maxIdx));
          types.add(rs.getString(typeIdx));
          if ("read:cpu".equals(key)) {
            Assert.assertEquals("1", rs.getString(minIdx));
            Assert.assertEquals("4", rs.getString(maxIdx));
          }
          Assert.assertNotNull(rs.getString(usedIdx));
        }
      }
      Assert.assertEquals("4", maxByType.get("read:cpu"));
      Assert.assertTrue(maxByType.containsKey("write:memory"));
      Assert.assertTrue(types.contains("temp_disk"));
      Assert.assertTrue(types.contains("disk_io"));

      Set<Integer> nodeIds = new HashSet<>();
      try (ResultSet rs = statement.executeQuery("SHOW USER QUOTA urq_user")) {
        int nodeIdx = columnIndex(rs.getMetaData(), "NodeID");
        while (rs.next()) {
          nodeIds.add(Integer.parseInt(rs.getString(nodeIdx)));
        }
      }
      Assert.assertFalse(nodeIds.isEmpty());
      for (Integer id : nodeIds) {
        Assert.assertTrue(id > 0);
      }

      statement.execute("DELETE USER QUOTA ON urq_user");
      try (ResultSet rs = statement.executeQuery("SHOW USER QUOTA urq_user")) {
        Assert.assertFalse(rs.next());
      }
    }
  }

  @Test
  public void lowerMaxDoesNotBreakShowAndNewAcquirePath() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_lower '" + USER_PASSWORD + "'");
      statement.execute("SET USER QUOTA ON urq_lower WITH read_cpu_max=8");
      statement.execute("SET USER QUOTA ON urq_lower WITH read_cpu_max=2");
      try (ResultSet rs = statement.executeQuery("SHOW USER QUOTA urq_lower")) {
        boolean seen = false;
        while (rs.next()) {
          if ("cpu".equalsIgnoreCase(rs.getString(columnIndex(rs.getMetaData(), "QuotaType")))
              && "read"
                  .equalsIgnoreCase(rs.getString(columnIndex(rs.getMetaData(), "Read/Write")))) {
            Assert.assertEquals("2", rs.getString(columnIndex(rs.getMetaData(), "Max")));
            seen = true;
          }
        }
        Assert.assertTrue(seen);
      }
    }
  }

  /** Reject SET on root; a later SET merges into existing bounds. */
  @Test
  public void rejectRootAndPartialUpdate() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("SET USER QUOTA ON root WITH read_cpu_max=1");
        Assert.fail("root SET should fail");
      } catch (Exception e) {
        // expected
      }

      statement.execute("CREATE USER urq_partial '" + USER_PASSWORD + "'");
      statement.execute(
          "SET USER QUOTA ON urq_partial WITH read_cpu_max=4, write_memory_max=10485760");
      statement.execute("SET USER QUOTA ON urq_partial WITH read_cpu_min=1");
      Map<String, String> values = new HashMap<>();
      try (ResultSet rs = statement.executeQuery("SHOW USER QUOTA urq_partial")) {
        ResultSetMetaData meta = rs.getMetaData();
        int rw = columnIndex(meta, "Read/Write");
        int type = columnIndex(meta, "QuotaType");
        int min = columnIndex(meta, "Min");
        int max = columnIndex(meta, "Max");
        while (rs.next()) {
          values.put(
              rs.getString(rw) + ":" + rs.getString(type),
              rs.getString(min) + "/" + rs.getString(max));
        }
      }
      Assert.assertEquals("1/4", values.get("read:cpu"));
      Assert.assertTrue(values.containsKey("write:memory"));
    }
  }

  /** Serial SELECT under read_cpu_max succeeds twice (acquire then release). */
  @Test
  public void readCpuMaxEnforcementAndRelease() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_cpu '" + USER_PASSWORD + "'");
      statement.execute("GRANT SELECT ON ANY TO USER urq_cpu");
      // DOP=4 may need several slots for a single query plan.
      statement.execute("SET USER QUOTA ON urq_cpu WITH read_cpu_max=8");
      statement.execute("CREATE DATABASE urq_cpu_db");
      statement.execute("USE urq_cpu_db");
      statement.execute("CREATE TABLE t1 (device STRING TAG, s1 INT32 FIELD)");
      statement.execute("INSERT INTO t1(device, time, s1) VALUES('d1', 1, 1)");

      try (Connection userCon =
              EnvFactory.getEnv()
                  .getConnection("urq_cpu", USER_PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
          Statement userStmt = userCon.createStatement()) {
        userStmt.execute("USE urq_cpu_db");
        try (ResultSet rs = userStmt.executeQuery("SELECT s1 FROM t1")) {
          Assert.assertTrue(rs.next());
        }
        try (ResultSet rs = userStmt.executeQuery("SELECT s1 FROM t1")) {
          Assert.assertTrue(rs.next());
        }
      }
    }
  }

  /** write_memory_max=1: INT32 insert estimate exceeds 1 byte, so a single INSERT is rejected. */
  @Test
  public void writeMemoryMaxRejectsInsert() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_wmem '" + USER_PASSWORD + "'");
      statement.execute("GRANT INSERT ON ANY TO USER urq_wmem");
      statement.execute("SET USER QUOTA ON urq_wmem WITH write_memory_max=1");
      statement.execute("CREATE DATABASE urq_wmem_db");
      statement.execute("USE urq_wmem_db");
      statement.execute("CREATE TABLE t1 (device STRING TAG, s1 INT32 FIELD)");

      assertThrowsQuota(
          () -> {
            try (Connection userCon =
                    EnvFactory.getEnv()
                        .getConnection("urq_wmem", USER_PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
                Statement userStmt = userCon.createStatement()) {
              userStmt.execute("USE urq_wmem_db");
              userStmt.execute(
                  "INSERT INTO t1(device, time, s1) VALUES('d1', 1, 1),('d1', 2, 2),('d1', 3, 3)");
            }
          },
          "write memory",
          "user max exceeded");
    }
  }

  /** write_disk_io_max is a bytes/sec throttle; burst inserts must hit size-limit exceeded. */
  @Test
  public void writeDiskIoMaxRejectsInsert() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_io '" + USER_PASSWORD + "'");
      statement.execute("GRANT INSERT ON ANY TO USER urq_io");
      statement.execute("SET USER QUOTA ON urq_io WITH write_disk_io_max=1");
      statement.execute("CREATE DATABASE urq_io_db");
      statement.execute("USE urq_io_db");
      statement.execute("CREATE TABLE t1 (device STRING TAG, s1 INT32 FIELD)");

      assertThrowsQuota(
          () -> {
            try (Connection userCon =
                    EnvFactory.getEnv()
                        .getConnection("urq_io", USER_PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
                Statement userStmt = userCon.createStatement()) {
              userStmt.execute("USE urq_io_db");
              // Several rounds so the rate limiter is exhausted.
              for (int i = 0; i < 20; i++) {
                userStmt.execute(
                    "INSERT INTO t1(device, time, s1) VALUES('d1', "
                        + (i * 5 + 1)
                        + ", 1),('d1', "
                        + (i * 5 + 2)
                        + ", 2),('d1', "
                        + (i * 5 + 3)
                        + ", 3),('d1', "
                        + (i * 5 + 4)
                        + ", 4),('d1', "
                        + (i * 5 + 5)
                        + ", 5)");
              }
            }
          },
          "write size limit exceeded");
    }
  }

  /** read_disk_io_max=1: each SELECT is charged ~1000B, so the rate limiter rejects. */
  @Test
  public void readDiskIoMaxRejectsSelect() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_rio '" + USER_PASSWORD + "'");
      statement.execute("GRANT SELECT ON ANY TO USER urq_rio");
      statement.execute("SET USER QUOTA ON urq_rio WITH read_disk_io_max=1");
      statement.execute("CREATE DATABASE urq_rio_db");
      statement.execute("USE urq_rio_db");
      statement.execute("CREATE TABLE t1 (device STRING TAG, s1 INT32 FIELD)");
      statement.execute("INSERT INTO t1(device, time, s1) VALUES('d1', 1, 1)");

      assertThrowsQuota(
          () -> {
            try (Connection userCon =
                    EnvFactory.getEnv()
                        .getConnection("urq_rio", USER_PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
                Statement userStmt = userCon.createStatement()) {
              userStmt.execute("USE urq_rio_db");
              userStmt.executeQuery("SELECT s1 FROM t1");
            }
          },
          "read size limit exceeded");
    }
  }

  /**
   * read_temp_disk_max is charged only on spill; table ORDER BY needs a larger series than tree
   * under the same sort_buffer. Retries with more rows if the first pass stayed in-memory.
   */
  @Test
  public void readTempDiskMaxRejectsSpillSort() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_td '" + USER_PASSWORD + "'");
      statement.execute("GRANT SELECT ON ANY TO USER urq_td");
      statement.execute("SET USER QUOTA ON urq_td WITH read_temp_disk_max=1024");
      statement.execute("CREATE DATABASE urq_td_db");
      statement.execute("USE urq_td_db");
      statement.execute("CREATE TABLE t1 (device STRING TAG, s1 INT32 FIELD)");

      int rows = 20000;
      for (int batch = 0; batch < rows / 500; batch++) {
        StringBuilder insert = new StringBuilder("INSERT INTO t1(device, time, s1) VALUES");
        for (int i = 1; i <= 500; i++) {
          if (i > 1) {
            insert.append(',');
          }
          int t = batch * 500 + i;
          insert.append("('d1', ").append(t).append(", ").append(rows - t).append(')');
        }
        statement.execute(insert.toString());
      }

      AssertionError last = null;
      for (int attempt = 0; attempt < 4; attempt++) {
        try {
          assertThrowsQuota(
              () -> {
                try (ITableSession userSession =
                    EnvFactory.getEnv().getTableSessionConnection("urq_td", USER_PASSWORD)) {
                  userSession.executeNonQueryStatement("USE urq_td_db");
                  try (SessionDataSet dataSet =
                      userSession.executeQueryStatement("SELECT s1 FROM t1 ORDER BY s1")) {
                    while (dataSet.hasNext()) {
                      dataSet.next();
                    }
                  }
                }
              },
              "temp_disk",
              "user max exceeded");
          return;
        } catch (AssertionError e) {
          last = e;
          // Still in-memory: grow the table in batches and retry spill.
          int base = rows + attempt * 10000;
          for (int batch = 0; batch < 20; batch++) {
            StringBuilder more = new StringBuilder("INSERT INTO t1(device, time, s1) VALUES");
            for (int i = 1; i <= 500; i++) {
              if (i > 1) {
                more.append(',');
              }
              int t = base + batch * 500 + i;
              more.append("('d1', ").append(t).append(", ").append(i).append(')');
            }
            statement.execute(more.toString());
          }
        }
      }
      throw last;
    }
  }

  private static void assertThrowsQuota(ThrowingRunnable action, String... mustContain)
      throws Exception {
    try {
      action.run();
      Assert.fail("expected quota rejection containing: " + String.join(", ", mustContain));
    } catch (Exception e) {
      if (!messageContains(e, mustContain)) {
        throw new AssertionError(
            "expected message containing "
                + String.join(", ", mustContain)
                + " but was: "
                + e.getMessage(),
            e);
      }
    }
  }

  private static boolean messageContains(Throwable t, String... mustContain) {
    StringBuilder sb = new StringBuilder();
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur.getMessage() != null) {
        sb.append(cur.getMessage()).append(' ');
      }
    }
    String msg = sb.toString().toLowerCase(Locale.ROOT);
    for (String part : mustContain) {
      if (!msg.contains(part.toLowerCase(Locale.ROOT))) {
        return false;
      }
    }
    return true;
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static int columnIndex(ResultSetMetaData meta, String label) throws Exception {
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      if (label.equalsIgnoreCase(meta.getColumnLabel(i))
          || label.equalsIgnoreCase(meta.getColumnName(i))) {
        return i;
      }
    }
    throw new AssertionError("column not found: " + label);
  }
}
