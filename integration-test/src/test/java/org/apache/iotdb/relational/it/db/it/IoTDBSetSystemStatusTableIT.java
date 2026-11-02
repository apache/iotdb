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
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.exception.InconsistentDataException;
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSetSystemStatusTableIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testReadOnlySqlAndDiskRecovery() throws Exception {
    try (Connection treeConnection = EnvFactory.getEnv().getConnection();
        Connection tableConnection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement tree = treeConnection.createStatement();
        Statement table = tableConnection.createStatement();
        // PREPARE and EXECUTE must use the same session, including with multi-node read checks.
        Statement prepared =
            (tableConnection instanceof ClusterTestConnection
                    ? ((ClusterTestConnection) tableConnection)
                        .writeConnection.getUnderlyingConnection()
                    : tableConnection)
                .createStatement()) {
      tree.execute("create database root.readonly_sql");
      tree.execute("create database root.readonly_drop");
      tree.execute("create timeseries root.readonly_sql.d.s with datatype=INT32,encoding=PLAIN");
      tree.execute("create timeseries root.readonly_drop.d.s with datatype=INT32,encoding=PLAIN");
      tree.execute("insert into root.readonly_sql.d(time,s) values(1,1)");
      tree.execute("insert into root.readonly_drop.d(time,s) values(1,1)");
      table.execute("create database readonly_sql");
      table.execute("create database readonly_drop");
      table.execute("create table readonly_drop.t(d STRING TAG,s INT32 FIELD)");
      table.execute("insert into readonly_drop.t(time,d,s) values(1,'d1',1)");
      table.execute("use readonly_sql");
      table.execute("create table t(d STRING TAG,a STRING ATTRIBUTE,s INT32 FIELD)");
      table.execute("create table prepared_t(d STRING TAG,a STRING ATTRIBUTE,s INT32 FIELD)");
      table.execute("insert into t(time,d,a,s) values(1,'d1','original',1)");
      tree.execute("flush");
      // Resolve both a read and a write before entering read-only mode.
      prepared.execute("prepare readonly_read from select s from t");
      prepared.execute(
          "prepare readonly_write from insert into prepared_t(time,d,a,s) "
              + "values(1,'p1','prepared',11),(2,'p2','prepared',12)");
      prepared.execute("execute readonly_write");
      table.execute("update prepared_t set a='changed' where d='p1'");
      prepared.execute("execute readonly_write");
      prepared.execute(
          "execute immediate 'insert into prepared_t(time,d,a,s) "
              + "values(3,''p3'',''immediate'',13)'");
      try (ResultSet result = table.executeQuery("select d,a,s from prepared_t order by time")) {
        for (int i = 1; i <= 3; i++) {
          Assert.assertTrue(result.next());
          Assert.assertEquals("p" + i, result.getString(1));
          Assert.assertEquals(i == 3 ? "immediate" : "prepared", result.getString(2));
          Assert.assertEquals(10 + i, result.getInt(3));
        }
        Assert.assertFalse(result.next());
      }
      try {
        table.execute("set system to readonly on cluster");
        awaitDataNodeStatus(table, "ReadOnly");

        for (String sql :
            new String[] {
              "show databases",
              "show timeseries root.readonly_sql.**",
              "show devices root.readonly_sql.**",
              "count timeseries root.readonly_sql.**",
              "show pipes",
              "show ttl on root.readonly_sql.**",
              "list user"
            }) {
          assertQueryAvailable(tree, sql);
        }
        for (String sql :
            new String[] {
              "show tables",
              "describe t",
              "show create table t",
              "show databases",
              "show pipes",
              "show devices from t",
              "count devices from t",
              "select * from information_schema.tables",
              "execute immediate 'select s from t'",
              "list user"
            }) {
          assertQueryAvailable(table, sql);
        }
        assertQueryAvailable(prepared, "execute readonly_read");
        assertQueryAvailable(prepared, "explain analyze execute readonly_read");
        try (ResultSet result = tree.executeQuery("select s from root.readonly_sql.d")) {
          Assert.assertTrue(result.next());
          Assert.assertEquals(1, result.getInt(2));
          Assert.assertFalse(result.next());
        }
        try (ResultSet result = table.executeQuery("select a,s from t where d='d1'")) {
          Assert.assertTrue(result.next());
          Assert.assertEquals("original", result.getString(1));
          Assert.assertEquals(1, result.getInt(2));
          Assert.assertFalse(result.next());
        }

        for (String sql :
            new String[] {
              "insert into root.readonly_sql.d(time,s) values(2,2)",
              "delete from root.readonly_sql.d.s where time<=1",
              "select s into root.readonly_dest.d(s) from root.readonly_sql.d",
              "create database root.readonly_new",
              "delete timeseries root.readonly_sql.d.s",
              "flush"
            }) {
          assertReadOnlyRejected(tree, sql);
        }
        for (String sql :
            new String[] {
              "insert into t(time,d,s) values(2,'d1',2)",
              "insert into t select * from t",
              "delete from t where time<=1",
              "update t set a='changed' where d='d1'",
              "create database readonly_new",
              "create table newtable(d STRING TAG,s INT32 FIELD)",
              "drop table t",
              "alter database readonly_sql set properties ttl=1, schema_region_group_num=2",
              "create role readonly_role",
              "execute immediate 'delete from t where time<=1'",
              "execute immediate 'insert into prepared_t(time,d,s) values(4,''p4'',14)'",
              "flush"
            }) {
          assertReadOnlyRejected(table, sql);
        }
        assertReadOnlyRejected(prepared, "execute readonly_write");
        assertReadOnlyRejected(prepared, "explain analyze execute readonly_write");

        tree.execute("set ttl to root.readonly_sql.** 1");
        tree.execute("delete database root.readonly_drop");
        table.execute("alter database readonly_sql set properties ttl=1");
        table.execute("alter table t set properties ttl=1");
        table.execute("drop database readonly_drop");
        try (ResultSet result = tree.executeQuery("show databases root.readonly_drop")) {
          Assert.assertFalse(result.next());
        }
        try (ResultSet result = table.executeQuery("show databases")) {
          while (result.next()) {
            Assert.assertNotEquals("readonly_drop", result.getString(1));
          }
        }
        // The recovery exceptions do not switch the system back to Running.
        awaitDataNodeStatus(table, "ReadOnly");
      } finally {
        table.execute("set system to running on cluster");
        awaitDataNodeStatus(table, "Running");
      }
      // Normal writes resume after leaving read-only mode.
      tree.execute("unset ttl to root.readonly_sql.**");
      table.execute("alter database readonly_sql set properties ttl=default");
      table.execute("alter table t set properties ttl=default");
      tree.execute("insert into root.readonly_sql.d(time,s) values(now(),2)");
      table.execute("insert into t(time,d,s) values(now(),'d1',2)");
      prepared.execute("deallocate prepare readonly_write");
      prepared.execute("deallocate prepare readonly_read");
    }
  }

  private static void awaitDataNodeStatus(Statement statement, String expectedStatus) {
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (ResultSet result = statement.executeQuery("show datanodes")) {
                int count = 0;
                while (result.next()) {
                  Assert.assertEquals(expectedStatus, result.getString("Status"));
                  count++;
                }
                Assert.assertEquals(EnvFactory.getEnv().getDataNodeWrapperList().size(), count);
              }
            });
  }

  private static void assertQueryAvailable(Statement statement, String sql) throws SQLException {
    try (ResultSet result = statement.executeQuery(sql)) {
      Assert.assertTrue(sql, result.getMetaData().getColumnCount() > 0);
      while (result.next()) {
        // Consume the full result to surface errors from distributed query execution.
      }
    }
  }

  private static void assertReadOnlyRejected(Statement statement, String sql) {
    try {
      statement.execute(sql);
      Assert.fail("Expected SYSTEM_READ_ONLY for " + sql);
    } catch (SQLException e) {
      Assert.assertEquals(sql, TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), e.getErrorCode());
    }
  }

  @Test
  public void setSystemStatus() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("SET SYSTEM TO READONLY ON CLUSTER");
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollDelay(1, TimeUnit.SECONDS)
          .until(
              () -> {
                ResultSet resultSet = statement.executeQuery("SHOW DATANODES");
                int num = 0;
                int manualReasonNum = 0;
                try {
                  while (resultSet.next()) {
                    String status = resultSet.getString("Status");
                    if (status.equals("ReadOnly")) {
                      num++;
                    }
                    // The ReadOnly requested through SQL is reported with the Manual reason.
                    // The StatusReason column only appears once at least one node has reported
                    // its reason, so a missing column means the condition is not satisfied yet.
                    if ("Manual".equals(resultSet.getString("StatusReason"))) {
                      manualReasonNum++;
                    }
                  }
                } catch (Exception e) {
                  // The StatusReason column (and the Manual reason behind it) may not be
                  // propagated yet, or the cluster may be transiently inconsistent while the
                  // status is changing: treat both as "not satisfied yet".
                  return false;
                }
                return num == EnvFactory.getEnv().getDataNodeWrapperList().size()
                    && manualReasonNum == EnvFactory.getEnv().getDataNodeWrapperList().size();
              });
      assertShowNodeColumns(true);

      // The table-model NODES view keeps the status and its reason in two separate columns
      // instead of merging them into "ReadOnly(Manual)".
      try (ResultSet nodesResultSet =
          statement.executeQuery("select * from information_schema.nodes")) {
        int dataNodeNum = 0;
        while (nodesResultSet.next()) {
          if ("DataNode".equals(nodesResultSet.getString("node_type"))) {
            dataNodeNum++;
            Assert.assertEquals("ReadOnly", nodesResultSet.getString("status"));
            Assert.assertEquals("Manual", nodesResultSet.getString("status_reason"));
          }
        }
        Assert.assertEquals(EnvFactory.getEnv().getDataNodeWrapperList().size(), dataNodeNum);
      }

      statement.execute("SET SYSTEM TO RUNNING ON CLUSTER");
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollDelay(1, TimeUnit.SECONDS)
          .until(
              () -> {
                ResultSet resultSet = statement.executeQuery("SHOW DATANODES");

                int num = 0;
                try {
                  if (resultSet.getMetaData().getColumnCount() != 6) {
                    return false;
                  }
                  while (resultSet.next()) {
                    String status = resultSet.getString("Status");
                    if (status.equals("Running")) {
                      num++;
                    }
                  }
                } catch (InconsistentDataException e) {
                  return false;
                }
                return num == EnvFactory.getEnv().getDataNodeWrapperList().size();
              });
      assertShowNodeColumns(false);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  private static void assertShowNodeColumns(boolean hasReason) throws Exception {
    for (String dialect : new String[] {BaseEnv.TREE_SQL_DIALECT, BaseEnv.TABLE_SQL_DIALECT}) {
      try (Connection connection = EnvFactory.getEnv().getConnection(dialect);
          Statement statement = connection.createStatement()) {
        assertShowColumns(statement, "SHOW CLUSTER", 7, hasReason);
        assertShowColumns(statement, "SHOW CLUSTER DETAILS", 13, hasReason);
        assertShowColumns(statement, "SHOW DATANODES", 6, hasReason);
        // SET SYSTEM changes DataNodes only; ConfigNodes keep the layout without a reason column.
        assertShowColumns(statement, "SHOW CONFIGNODES", 5, false);
      }
    }
  }

  private static void assertShowColumns(
      Statement statement, String sql, int baseColumnCount, boolean hasReason) throws Exception {
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      int columnCount = resultSet.getMetaData().getColumnCount();
      Assert.assertEquals(sql, baseColumnCount + (hasReason ? 1 : 0), columnCount);
      if (hasReason) {
        Assert.assertEquals(
            sql, "StatusReason", resultSet.getMetaData().getColumnName(columnCount));
      }
      Assert.assertTrue(sql, resultSet.next());
      do {
        if (hasReason) {
          String expectedReason =
              sql.startsWith("SHOW CLUSTER") && !"DataNode".equals(resultSet.getString("NodeType"))
                  ? null
                  : "Manual";
          Assert.assertEquals(sql, expectedReason, resultSet.getString(columnCount));
        }
      } while (resultSet.next());
    }
  }
}
