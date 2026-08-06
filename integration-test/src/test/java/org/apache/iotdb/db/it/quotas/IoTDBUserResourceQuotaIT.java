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
import java.util.Map;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUserResourceQuotaIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setQuotaEnable(true);
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
      statement.execute("CREATE USER quota_user 'pass'");
      statement.execute("GRANT READ_DATA ON root.** TO USER quota_user");
      statement.execute("SET THROTTLE QUOTA cpu=1 ON quota_user");
      statement.execute("CREATE DATABASE root.quota");
      statement.execute("CREATE TIMESERIES root.quota.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.quota.d1(time, s1) VALUES (1, 1)");
      try (ResultSet rs = statement.executeQuery("SHOW THROTTLE QUOTA quota_user")) {
        Assert.assertTrue(rs.next());
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("quota_user", "pass");
          Statement userStmt = userCon.createStatement()) {
        userStmt.executeQuery("SELECT s1 FROM root.quota.d1");
      }
    }
  }

  @Test
  public void setAndShowUserQuota() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_user 'pass'");
      statement.execute(
          "SET USER QUOTA ON urq_user WITH read_cpu_min=1, read_cpu_max=4, write_memory_max=10M, write_disk_io_max=10485760, write_temp_disk_max=8589934592");
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
      statement.execute("CREATE USER urq_lower 'pass'");
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

      statement.execute("CREATE USER urq_partial 'pass'");
      statement.execute("SET USER QUOTA ON urq_partial WITH read_cpu_max=4, write_memory_max=10M");
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

      statement.execute("CREATE USER urq_cross 'pass'");
      statement.execute("SET THROTTLE QUOTA cpu=2 ON urq_cross");
      statement.execute("SET USER QUOTA ON urq_cross WITH write_disk_io_max=1048576");
      Assert.assertTrue(hasQuotaType(statement, "SHOW USER QUOTA urq_cross", "cpu"));
      Assert.assertTrue(hasQuotaType(statement, "SHOW USER QUOTA urq_cross", "disk_io"));
      Assert.assertTrue(hasAnyRow(statement, "SHOW THROTTLE QUOTA urq_cross"));
    }
  }

  @Test
  public void readCpuMaxEnforcementAndRelease() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER urq_cpu 'pass'");
      statement.execute("GRANT READ_DATA ON root.** TO USER urq_cpu");
      statement.execute("SET USER QUOTA ON urq_cpu WITH read_cpu_max=1");
      statement.execute("CREATE DATABASE root.urq_cpu");
      statement.execute("CREATE TIMESERIES root.urq_cpu.d1.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("INSERT INTO root.urq_cpu.d1(time,s1) VALUES(1,1)");

      // Single query should succeed (and release).
      try (Connection userCon = EnvFactory.getEnv().getConnection("urq_cpu", "pass");
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
