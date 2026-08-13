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

package org.apache.iotdb.db.it.quotas;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

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
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUserResourceQuotaIT {

  private static final String USER_PASSWORD = "IoTDB@2021abc";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setQuotaEnable(true);
    // Small sort buffer forces external-sort spill for read_temp_disk_*. sortBuffer > maxTsBlock.
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024L);
    // Multiple pipeline drivers per FI so read_cpu_max=1 can reject a single query
    // deterministically.
    EnvFactory.getEnv().getConfig().getCommonConfig().setDegreeOfParallelism(4);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setQuotaEnable(false);
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void throttleQuotaMapsToReadCpuMax() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER quota_user '" + USER_PASSWORD + "'");
      statement.execute("SET THROTTLE QUOTA cpu=1 ON quota_user");
      try (ResultSet rs = statement.executeQuery("SHOW THROTTLE QUOTA quota_user")) {
        boolean sawCpuLimit = false;
        while (rs.next()) {
          ResultSetMetaData meta = rs.getMetaData();
          if ("cpu".equalsIgnoreCase(rs.getString(columnIndex(meta, "QuotaType")))
              && "1".equals(rs.getString(columnIndex(meta, "Limit")))) {
            sawCpuLimit = true;
          }
        }
        Assert.assertTrue("SHOW THROTTLE QUOTA should expose cpu limit=1", sawCpuLimit);
      }
      // Assert mapping via SHOW USER QUOTA. Do not fan out a user SELECT under ClusterIT: with
      // class-level DOP>1 and read_cpu_max=1, DataNodes may reject inconsistently
      // (InconsistentDataException).
      boolean sawReadCpuMax = false;
      try (ResultSet rs = statement.executeQuery("SHOW USER QUOTA quota_user")) {
        while (rs.next()) {
          ResultSetMetaData meta = rs.getMetaData();
          if ("read".equalsIgnoreCase(rs.getString(columnIndex(meta, "Read/Write")))
              && "cpu".equalsIgnoreCase(rs.getString(columnIndex(meta, "QuotaType")))) {
            Assert.assertEquals("1", rs.getString(columnIndex(meta, "Max")));
            sawReadCpuMax = true;
          }
        }
      }
      Assert.assertTrue(
          "SET THROTTLE QUOTA cpu=1 should map to SHOW USER QUOTA read:cpu max=1", sawReadCpuMax);
    }
  }

  /** SET / SHOW / DELETE USER QUOTA: column shape, resource types, and NodeID aggregation. */
  @Test
  public void setAndShowUserQuota() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
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

      // At least one alive DataNode id appears in SHOW (dedicated usage-report aggregation).
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
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

  /** Reject SET on root; partial SET merges; USER QUOTA and THROTTLE QUOTA can coexist. */
  @Test
  public void rejectRootAndPartialUpdateAndCrossThrottle() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("SET USER QUOTA ON `root` WITH read_cpu_max=1");
        Assert.fail("root SET should fail");
      } catch (Exception e) {
        // expected
      }

      // Partial update: later SET only fills missing bounds, keeps existing max.
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

      // Cross-path: throttle cpu + user-resource disk_io both visible.
      statement.execute("CREATE USER urq_cross '" + USER_PASSWORD + "'");
      statement.execute("SET THROTTLE QUOTA cpu=2 ON urq_cross");
      statement.execute("SET USER QUOTA ON urq_cross WITH write_disk_io_max=1048576");
      Assert.assertTrue(hasQuotaType(statement, "SHOW USER QUOTA urq_cross", "cpu"));
      Assert.assertTrue(hasQuotaType(statement, "SHOW USER QUOTA urq_cross", "disk_io"));
      Assert.assertTrue(hasAnyRow(statement, "SHOW THROTTLE QUOTA urq_cross"));
    }
  }

  /** Serial SELECT under read_cpu_max succeeds twice (acquire then release). */
  @Test
  public void readCpuMaxEnforcementAndRelease() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_cpu '" + USER_PASSWORD + "'");
      statement.execute("GRANT READ_DATA ON root.** TO USER urq_cpu");
      // DOP=4 may create multiple drivers; allow enough slots for serial success + release.
      statement.execute("SET USER QUOTA ON urq_cpu WITH read_cpu_max=8");
      statement.execute("CREATE DATABASE root.urq_cpu");
      statement.execute("CREATE TIMESERIES root.urq_cpu.d1.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("INSERT INTO root.urq_cpu.d1(time,s1) VALUES(1,1)");

      try (Connection userCon = EnvFactory.getEnv().getConnection("urq_cpu", USER_PASSWORD);
          Statement userStmt = userCon.createStatement()) {
        try (ResultSet rs = userStmt.executeQuery("SELECT s1 FROM root.urq_cpu.d1")) {
          Assert.assertTrue(rs.next());
        }
        try (ResultSet rs = userStmt.executeQuery("SELECT s1 FROM root.urq_cpu.d1")) {
          Assert.assertTrue(rs.next());
        }
      }
    }
  }

  /** write_memory_max=1: INT32 insert estimate exceeds 1 byte, so a single INSERT is rejected. */
  @Test
  public void writeMemoryMaxRejectsInsert() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_wmem '" + USER_PASSWORD + "'");
      statement.execute("GRANT WRITE_DATA ON root.** TO USER urq_wmem");
      statement.execute("SET USER QUOTA ON urq_wmem WITH write_memory_max=1");
      statement.execute("CREATE DATABASE root.urq_wmem");
      statement.execute("CREATE TIMESERIES root.urq_wmem.d1.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");

      assertThrowsQuota(
          () -> {
            try (Connection userCon = EnvFactory.getEnv().getConnection("urq_wmem", USER_PASSWORD);
                Statement userStmt = userCon.createStatement()) {
              userStmt.execute("INSERT INTO root.urq_wmem.d1(time,s1) VALUES(1,1),(2,2),(3,3)");
            }
          },
          "write memory",
          "user max exceeded");
    }
  }

  /**
   * write_disk_io_max is a bytes/sec throttle; burst inserts must hit "write size limit exceeded".
   */
  @Test
  public void writeDiskIoMaxRejectsInsert() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_io '" + USER_PASSWORD + "'");
      statement.execute("GRANT WRITE_DATA ON root.** TO USER urq_io");
      statement.execute("SET USER QUOTA ON urq_io WITH write_disk_io_max=1");
      statement.execute("CREATE DATABASE root.urq_io");
      statement.execute("CREATE TIMESERIES root.urq_io.d1.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");

      assertThrowsQuota(
          () -> {
            try (Connection userCon = EnvFactory.getEnv().getConnection("urq_io", USER_PASSWORD);
                Statement userStmt = userCon.createStatement()) {
              // Multiple rounds so rate limiter is exhausted even if first grab partially succeeds.
              for (int i = 0; i < 20; i++) {
                userStmt.execute(
                    "INSERT INTO root.urq_io.d1(time,s1) VALUES("
                        + (i * 5 + 1)
                        + ",1),("
                        + (i * 5 + 2)
                        + ",2),("
                        + (i * 5 + 3)
                        + ",3),("
                        + (i * 5 + 4)
                        + ",4),("
                        + (i * 5 + 5)
                        + ",5)");
              }
            }
          },
          "write size limit exceeded");
    }
  }

  /** read_disk_io_max=1: each SELECT is charged ~1000B, so the rate limiter rejects. */
  @Test
  public void readDiskIoMaxRejectsSelect() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_rio '" + USER_PASSWORD + "'");
      statement.execute("GRANT READ_DATA ON root.** TO USER urq_rio");
      statement.execute("SET USER QUOTA ON urq_rio WITH read_disk_io_max=1");
      statement.execute("CREATE DATABASE root.urq_rio");
      statement.execute("CREATE TIMESERIES root.urq_rio.d1.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("INSERT INTO root.urq_rio.d1(time,s1) VALUES(1,1)");

      assertThrowsQuota(
          () -> {
            try (Connection userCon = EnvFactory.getEnv().getConnection("urq_rio", USER_PASSWORD);
                Statement userStmt = userCon.createStatement()) {
              userStmt.executeQuery("SELECT s1 FROM root.urq_rio.d1");
            }
          },
          "read size limit exceeded");
    }
  }

  /**
   * read_temp_disk_max is charged only on real spill; ORDER BY with data above sort_buffer must
   * fail. Retries with more rows if the first pass stayed in-memory.
   */
  @Test
  public void readTempDiskMaxRejectsSpillSort() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_td '" + USER_PASSWORD + "'");
      statement.execute("GRANT READ_DATA ON root.** TO USER urq_td");
      statement.execute("SET USER QUOTA ON urq_td WITH read_temp_disk_max=1024");
      statement.execute("CREATE DATABASE root.urq_td");
      statement.execute("CREATE TIMESERIES root.urq_td.d1.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");

      int rows = 5000;
      for (int batch = 0; batch < rows / 500; batch++) {
        StringBuilder insert = new StringBuilder("INSERT INTO root.urq_td.d1(time,s1) VALUES");
        for (int i = 1; i <= 500; i++) {
          if (i > 1) {
            insert.append(',');
          }
          int t = batch * 500 + i;
          insert.append('(').append(t).append(',').append(rows - t).append(')');
        }
        statement.execute(insert.toString());
      }

      AssertionError last = null;
      for (int attempt = 0; attempt < 3; attempt++) {
        try {
          assertThrowsQuota(
              () -> {
                try (Connection userCon =
                        EnvFactory.getEnv().getConnection("urq_td", USER_PASSWORD);
                    Statement userStmt = userCon.createStatement()) {
                  userStmt.executeQuery("SELECT s1 FROM root.urq_td.d1 ORDER BY s1");
                }
              },
              "temp_disk",
              "user max exceeded");
          return;
        } catch (AssertionError e) {
          last = e;
          // Still in-memory: grow the series and retry spill.
          StringBuilder more = new StringBuilder("INSERT INTO root.urq_td.d1(time,s1) VALUES");
          int base = rows * (attempt + 2);
          for (int i = 1; i <= rows; i++) {
            if (i > 1) {
              more.append(',');
            }
            more.append('(').append(base + i).append(',').append(i).append(')');
          }
          statement.execute(more.toString());
        }
      }
      throw last;
    }
  }

  @Test
  public void setQuotaOnNonExistentUserFails() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("SET USER QUOTA ON no_such_user WITH read_cpu_max=1");
        Assert.fail("SET USER QUOTA on non-existent user should fail");
      } catch (Exception e) {
        Assert.assertTrue(
            "expected USER_NOT_EXIST style message but was: " + e.getMessage(),
            messageContains(e, "No such user", "no_such_user"));
      }
    }
  }

  /** DROP USER clears quota keyed by userId; a recreated same name starts with no quota. */
  @Test
  public void dropUserClearsQuotaAndRecreateStartsClean() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_drop '" + USER_PASSWORD + "'");
      statement.execute("SET USER QUOTA ON urq_drop WITH read_cpu_max=4");
      Assert.assertTrue(hasAnyRow(statement, "SHOW USER QUOTA urq_drop"));
      statement.execute("DROP USER urq_drop");
      // Re-created user gets a new userId and must not inherit the old quota.
      statement.execute("CREATE USER urq_drop '" + USER_PASSWORD + "'");
      Assert.assertFalse(hasAnyRow(statement, "SHOW USER QUOTA urq_drop"));
    }
  }

  /** ALTER USER RENAME keeps quota (keyed by immutable userId), old name shows nothing. */
  @Test
  public void renameUserKeepsQuota() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_old '" + USER_PASSWORD + "'");
      statement.execute("SET USER QUOTA ON urq_old WITH read_cpu_min=1, read_cpu_max=4");
      statement.execute("ALTER USER urq_old RENAME TO urq_new");
      // Quota is keyed by the immutable userId, so it follows the user to the new name.
      Map<String, String> values = new HashMap<>();
      try (ResultSet rs = statement.executeQuery("SHOW USER QUOTA urq_new")) {
        ResultSetMetaData meta = rs.getMetaData();
        int userIdx = columnIndex(meta, "User");
        int rw = columnIndex(meta, "Read/Write");
        int type = columnIndex(meta, "QuotaType");
        int max = columnIndex(meta, "Max");
        while (rs.next()) {
          Assert.assertEquals("urq_new", rs.getString(userIdx));
          values.put(rs.getString(rw) + ":" + rs.getString(type), rs.getString(max));
        }
      }
      Assert.assertEquals("4", values.get("read:cpu"));
      // The old name no longer resolves to any user, so SHOW returns nothing.
      Assert.assertFalse(hasAnyRow(statement, "SHOW USER QUOTA urq_old"));
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

  private static boolean hasQuotaType(Statement statement, String sql, String quotaType)
      throws Exception {
    try (ResultSet rs = statement.executeQuery(sql)) {
      int typeIdx = columnIndex(rs.getMetaData(), "QuotaType");
      while (rs.next()) {
        if (quotaType.equalsIgnoreCase(rs.getString(typeIdx))) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean hasAnyRow(Statement statement, String sql) throws Exception {
    try (ResultSet rs = statement.executeQuery(sql)) {
      return rs.next();
    }
  }

  private static int columnIndex(ResultSetMetaData meta, String name) throws Exception {
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      if (name.equalsIgnoreCase(meta.getColumnLabel(i))
          || name.equalsIgnoreCase(meta.getColumnName(i))) {
        return i;
      }
    }
    throw new AssertionError("column not found: " + name);
  }
}
