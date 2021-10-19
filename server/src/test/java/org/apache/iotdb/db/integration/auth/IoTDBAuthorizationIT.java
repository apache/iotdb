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
package org.apache.iotdb.db.integration.auth;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBAuthorizationIT {

  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void allPrivilegesTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw'");
      setUpForTriggerRelatedPrivilegesTests(adminStmt);
      boolean caught = false;
      try (Connection userCon =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        try {
          userStmt.execute("SET STORAGE GROUP TO root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (100, 100)");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          userStmt.execute("SELECT * from root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          userStmt.execute("GRANT USER tempuser PRIVILEGES CREATE_TIMESERIES ON root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("GRANT USER tempuser PRIVILEGES ALL ON root");

        userStmt.execute("SET STORAGE GROUP TO root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (100, 100)");
        userStmt.execute("SELECT * from root.a");
        userStmt.execute("GRANT USER tempuser PRIVILEGES SET_STORAGE_GROUP ON root.a");
        userStmt.execute("GRANT USER tempuser PRIVILEGES CREATE_TIMESERIES ON root.b.b");

        adminStmt.execute("REVOKE USER tempuser PRIVILEGES ALL ON root");
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES CREATE_TIMESERIES ON root.b.b");

        caught = false;
        try {
          userStmt.execute("SET STORAGE GROUP TO root.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          userStmt.execute("CREATE TIMESERIES root.b.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          userStmt.execute("INSERT INTO root.b(timestamp, b) VALUES (100, 100)");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          userStmt.execute("SELECT * from root.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          userStmt.execute("GRANT USER tempuser PRIVILEGES CREATE_TIMESERIES ON root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        executeTriggerRelatedPrivilegesTests(userStmt);
      }
    }
  }

  private static void setUpForTriggerRelatedPrivilegesTests(Statement adminStmt)
      throws SQLException {
    adminStmt.execute(
        "create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE");
    adminStmt.execute(
        "create timeseries root.ln.wf02.wt02.hardware with datatype=TEXT,encoding=PLAIN");
    adminStmt.execute(
        "create timeseries root.ln.wf03.wt03.software with datatype=DOUBLE,encoding=PLAIN");
    adminStmt.execute(
        "create trigger started-trigger before insert on root.ln.wf01.wt01.temperature as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
    adminStmt.execute(
        "create trigger stopped-trigger after insert on root.ln.wf02.wt02.hardware as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
    adminStmt.execute("stop trigger stopped-trigger");
  }

  private static void executeTriggerRelatedPrivilegesTests(Statement userStmt) throws SQLException {
    String[] statements = {
      "create trigger magic before insert on root.ln.wf03.wt03.software as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'",
      "stop trigger started-trigger",
      "drop trigger started-trigger",
      "start trigger stopped-trigger",
    };
    for (String statement : statements) {
      try {
        userStmt.execute(statement);
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("602"));
        continue;
      }
      fail();
    }
  }

  @Test
  public void illegalPasswordTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    boolean caught = false;
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw '");
    } catch (SQLException e) {
      caught = true;
    }
    assertTrue(caught);

    caught = false;
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'te'");
    } catch (SQLException e) {
      caught = true;
    }
    assertTrue(caught);
  }

  @Test
  public void updatePasswordTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw'");
      Connection userCon =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
      userCon.close();

      adminStmt.execute("ALTER USER tempuser SET PASSWORD 'newpw'");
      boolean caught = false;
      try {
        userCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
      } catch (SQLException e) {
        caught = true;
      } finally {
        userCon.close();
      }
      assertTrue(caught);

      userCon =
          DriverManager.getConnection(
              Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "newpw");

      userCon.close();
    }
  }

  @Test
  public void illegalGrantRevokeUserTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw'");

      try (Connection userCon =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        // grant a non-existing user
        boolean caught = false;
        try {
          adminStmt.execute("GRANT USER nulluser PRIVILEGES SET_STORAGE_GROUP on root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // grant a non-existing privilege
        caught = false;
        try {
          adminStmt.execute("GRANT USER tempuser PRIVILEGES NOT_A_PRIVILEGE on root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // duplicate grant
        adminStmt.execute("GRANT USER tempuser PRIVILEGES CREATE_USER on root.a");
        caught = false;
        try {
          adminStmt.execute("GRANT USER tempuser PRIVILEGES CREATE_USER on root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // grant on a illegal seriesPath
        caught = false;
        try {
          adminStmt.execute("GRANT USER tempuser PRIVILEGES DELETE_TIMESERIES on a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // grant admin
        caught = false;
        try {
          adminStmt.execute("GRANT USER root PRIVILEGES DELETE_TIMESERIES on root.a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // no privilege to grant
        caught = false;
        try {
          userStmt.execute("GRANT USER tempuser PRIVILEGES DELETE_TIMESERIES on root.a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // revoke a non-existing privilege
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES CREATE_USER on root.a");
        caught = false;
        try {
          adminStmt.execute("REVOKE USER tempuser PRIVILEGES CREATE_USER on root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // revoke a non-existing user
        caught = false;
        try {
          adminStmt.execute("REVOKE USER tempuser1 PRIVILEGES CREATE_USER on root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // revoke on a illegal seriesPath
        caught = false;
        try {
          adminStmt.execute("REVOKE USER tempuser PRIVILEGES DELETE_TIMESERIES on a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // revoke admin
        caught = false;
        try {
          adminStmt.execute("REVOKE USER root PRIVILEGES DELETE_TIMESERIES on root.a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // no privilege to revoke
        caught = false;
        try {
          userStmt.execute("REVOKE USER tempuser PRIVILEGES DELETE_TIMESERIES on root.a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // grant privilege to grant
        caught = false;
        try {
          userStmt.execute("GRANT USER tempuser PRIVILEGES DELETE_TIMESERIES on root.a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES GRANT_USER_PRIVILEGE on root");
        userStmt.execute("GRANT USER tempuser PRIVILEGES DELETE_TIMESERIES on root");

        // grant privilege to revoke
        caught = false;
        try {
          userStmt.execute("REVOKE USER tempuser PRIVILEGES DELETE_TIMESERIES on root");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES REVOKE_USER_PRIVILEGE on root");
        userStmt.execute("REVOKE USER tempuser PRIVILEGES DELETE_TIMESERIES on root");
      }
    }
  }

  @Test
  public void createDeleteTimeSeriesTest() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {

      adminStmt.execute("CREATE USER tempuser 'temppw'");

      try (Connection userCon =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        // grant and revoke the user the privilege to create time series
        boolean caught = false;
        try {
          userStmt.execute("SET STORAGE GROUP TO root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("GRANT USER tempuser PRIVILEGES SET_STORAGE_GROUP ON root.a");
        userStmt.execute("SET STORAGE GROUP TO root.a");
        adminStmt.execute("GRANT USER tempuser PRIVILEGES CREATE_TIMESERIES ON root.a.b");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");

        caught = false;
        try {
          // no privilege to create this one
          userStmt.execute("SET STORAGE GROUP TO root.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          // privilege already exists
          adminStmt.execute("GRANT USER tempuser PRIVILEGES SET_STORAGE_GROUP ON root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("REVOKE USER tempuser PRIVILEGES SET_STORAGE_GROUP ON root.a");
        caught = false;
        try {
          // no privilege to create this one any more
          userStmt.execute("SET STORAGE GROUP TO root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          // no privilege to create timeseries
          userStmt.execute("CREATE TIMESERIES root.b.a WITH DATATYPE=INT32,ENCODING=PLAIN");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
          // privilege already exists
          adminStmt.execute("GRANT USER tempuser PRIVILEGES CREATE_TIMESERIES ON root.a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("REVOKE USER tempuser PRIVILEGES CREATE_TIMESERIES ON root.a.b");
        caught = false;
        try {
          // no privilege to create this one any more
          userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // the user cannot delete the timeseries now
        caught = false;
        try {
          // no privilege to delete this one any more
          userStmt.execute("DELETE TIMESERIES root.a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // the user can delete the timeseries now
        adminStmt.execute("GRANT USER tempuser PRIVILEGES DELETE_TIMESERIES on root.a");
        adminStmt.execute("GRANT USER tempuser PRIVILEGES DELETE_TIMESERIES on root.b");
        userStmt.execute("DELETE TIMESERIES root.a.b");

        // revoke the privilege to delete time series
        adminStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        adminStmt.execute("SET STORAGE GROUP TO root.b");
        adminStmt.execute("CREATE TIMESERIES root.b.a WITH DATATYPE=INT32,ENCODING=PLAIN");
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES DELETE_TIMESERIES on root.a");
        userStmt.execute("DELETE TIMESERIES root.b.a");
        caught = false;
        try {
          // no privilege to create this one any more
          userStmt.execute("DELETE TIMESERIES root.a.b");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);
      }
    }
  }

  @Test
  public void insertQueryTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw'");

      try (Connection userCon =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        adminStmt.execute("GRANT USER tempuser PRIVILEGES SET_STORAGE_GROUP ON root.a");
        userStmt.execute("SET STORAGE GROUP TO root.a");
        adminStmt.execute("GRANT USER tempuser PRIVILEGES CREATE_TIMESERIES ON root.a.b");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");

        // grant privilege to insert
        boolean caught = false;
        try {
          userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES INSERT_TIMESERIES on root.a");
        userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");

        // revoke privilege to insert
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES INSERT_TIMESERIES on root.a");
        caught = false;
        try {
          userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        // grant privilege to query
        caught = false;
        try {
          userStmt.execute("SELECT * from root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES READ_TIMESERIES on root.a");
        userStmt.execute("SELECT * from root.a");
        userStmt.getResultSet().close();
        userStmt.execute("SELECT LAST b from root.a");
        userStmt.getResultSet().close();

        // revoke privilege to query
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES READ_TIMESERIES on root.a");
        caught = false;
        try {
          userStmt.execute("SELECT * from root.a");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);
      }
    }
  }

  @Test
  public void rolePrivilegeTest() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {

      adminStmt.execute("CREATE USER tempuser 'temppw'");
      try (Connection userCon =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        boolean caught = false;
        try {
          userStmt.execute("CREATE ROLE admin");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("CREATE ROLE admin");
        adminStmt.execute(
            "GRANT ROLE admin PRIVILEGES SET_STORAGE_GROUP,CREATE_TIMESERIES,DELETE_TIMESERIES,READ_TIMESERIES,INSERT_TIMESERIES on root");
        adminStmt.execute("GRANT admin TO tempuser");

        userStmt.execute("SET STORAGE GROUP TO root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("CREATE TIMESERIES root.a.c WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.a(timestamp,b,c) VALUES (1,100,1000)");
        // userStmt.execute("DELETE FROM root.a.b WHERE TIME <= 1000000000");
        userStmt.execute("SELECT * FROM root.**");
        userStmt.getResultSet().close();

        adminStmt.execute("REVOKE ROLE admin PRIVILEGES DELETE_TIMESERIES on root");
        caught = false;
        try {
          userStmt.execute("DELETE FROM root.* WHERE TIME <= 1000000000");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("GRANT USER tempuser PRIVILEGES READ_TIMESERIES on root");
        adminStmt.execute("REVOKE admin FROM tempuser");
        userStmt.execute("SELECT * FROM root.**");
        userStmt.getResultSet().close();
        caught = false;
        try {
          userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        } catch (SQLException e) {
          caught = true;
        }
        assertTrue(caught);
      }
    }
  }

  @Test
  @Ignore
  public void authPerformanceTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {

      adminStmt.execute("CREATE USER tempuser 'temppw'");
      adminStmt.execute("SET STORAGE GROUP TO root.a");
      int privilegeCnt = 500;
      for (int i = 0; i < privilegeCnt; i++) {
        adminStmt.execute("CREATE TIMESERIES root.a.b" + i + " WITH DATATYPE=INT32,ENCODING=PLAIN");
        adminStmt.execute("GRANT USER tempuser PRIVILEGES INSERT_TIMESERIES ON root.a.b" + i);
      }
      try (Connection userCon =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        int insertCnt = 20000;
        int batchSize = 500;

        long time = System.currentTimeMillis();
        for (int i = 0; i < insertCnt; ) {
          for (int j = 0; j < batchSize; j++) {
            userStmt.addBatch(
                "INSERT INTO root.a(timestamp, b"
                    + (privilegeCnt - 1)
                    + ") VALUES ("
                    + (i++ + 1)
                    + ", 100)");
          }
          userStmt.executeBatch();
          userStmt.clearBatch();
        }

        time = System.currentTimeMillis();
        for (int i = 0; i < insertCnt; ) {
          for (int j = 0; j < batchSize; j++) {
            adminStmt.addBatch(
                "INSERT INTO root.a(timestamp, b0) VALUES (" + (i++ + 1 + insertCnt) + ", 100)");
          }
          adminStmt.executeBatch();
          adminStmt.clearBatch();
        }
      }
    }
  }

  @Test
  public void testListUser() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection adminCon =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement adminStmt = adminCon.createStatement();

    try {
      ResultSet resultSet = adminStmt.executeQuery("LIST USER");
      String ans = String.format("root,\n");
      try {
        validateResultSet(resultSet, ans);

        for (int i = 0; i < 10; i++) {
          adminStmt.execute("CREATE USER user" + i + " 'password" + i + "'");
        }
        resultSet = adminStmt.executeQuery("LIST USER");
        ans =
            "root,\n"
                + "user0,\n"
                + "user1,\n"
                + "user2,\n"
                + "user3,\n"
                + "user4,\n"
                + "user5,\n"
                + "user6,\n"
                + "user7,\n"
                + "user8,\n"
                + "user9,\n";
        validateResultSet(resultSet, ans);

        for (int i = 0; i < 10; i++) {
          if (i % 2 == 0) {
            adminStmt.execute("DROP USER user" + i);
          }
        }
        resultSet = adminStmt.executeQuery("LIST USER");
        ans = "root,\n" + "user1,\n" + "user3,\n" + "user5,\n" + "user7,\n" + "user9,\n";
        validateResultSet(resultSet, ans);
      } finally {
        resultSet.close();
      }
    } finally {
      adminCon.close();
    }
  }

  @Test
  public void testListRole() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection adminCon =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement adminStmt = adminCon.createStatement();

    try {
      ResultSet resultSet = adminStmt.executeQuery("LIST ROLE");
      String ans = "";
      try {
        validateResultSet(resultSet, ans);

        for (int i = 0; i < 10; i++) {
          adminStmt.execute("CREATE ROLE role" + i);
        }

        resultSet = adminStmt.executeQuery("LIST ROLE");
        ans =
            "role0,\n"
                + "role1,\n"
                + "role2,\n"
                + "role3,\n"
                + "role4,\n"
                + "role5,\n"
                + "role6,\n"
                + "role7,\n"
                + "role8,\n"
                + "role9,\n";
        validateResultSet(resultSet, ans);

        for (int i = 0; i < 10; i++) {
          if (i % 2 == 0) {
            adminStmt.execute("DROP ROLE role" + i);
          }
        }
        resultSet = adminStmt.executeQuery("LIST ROLE");
        ans = "role1,\n" + "role3,\n" + "role5,\n" + "role7,\n" + "role9,\n";
        validateResultSet(resultSet, ans);
      } finally {
        resultSet.close();
      }
    } finally {
      adminStmt.close();
      adminCon.close();
    }
  }

  @Test
  public void testListUserPrivileges() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection adminCon =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement adminStmt = adminCon.createStatement();

    try {
      adminStmt.execute("CREATE USER user1 'password1'");
      adminStmt.execute("GRANT USER user1 PRIVILEGES READ_TIMESERIES ON root.a.b");
      adminStmt.execute("CREATE ROLE role1");
      adminStmt.execute(
          "GRANT ROLE role1 PRIVILEGES READ_TIMESERIES,INSERT_TIMESERIES,DELETE_TIMESERIES ON root.a.b.c");
      adminStmt.execute(
          "GRANT ROLE role1 PRIVILEGES READ_TIMESERIES,INSERT_TIMESERIES,DELETE_TIMESERIES ON root.d.b.c");
      adminStmt.execute("GRANT role1 TO user1");

      ResultSet resultSet = adminStmt.executeQuery("LIST USER PRIVILEGES  user1");
      String ans =
          ",root.a.b : READ_TIMESERIES"
              + ",\n"
              + "role1,root.a.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES"
              + ",\n"
              + "role1,root.d.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES"
              + ",\n";
      try {
        validateResultSet(resultSet, ans);

        resultSet = adminStmt.executeQuery("LIST PRIVILEGES USER user1 ON root.a.b.c");
        ans =
            ",root.a.b : READ_TIMESERIES"
                + ",\n"
                + "role1,root.a.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES"
                + ",\n";
        validateResultSet(resultSet, ans);

        adminStmt.execute("REVOKE role1 from user1");

        resultSet = adminStmt.executeQuery("LIST USER PRIVILEGES  user1");
        ans = ",root.a.b : READ_TIMESERIES,\n";
        validateResultSet(resultSet, ans);

        resultSet = adminStmt.executeQuery("LIST PRIVILEGES USER user1 ON root.a.b.c");
        ans = ",root.a.b : READ_TIMESERIES,\n";
        validateResultSet(resultSet, ans);
      } finally {
        resultSet.close();
      }
    } finally {
      adminStmt.close();
      adminCon.close();
    }
  }

  @Test
  public void testListRolePrivileges() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection adminCon =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement adminStmt = adminCon.createStatement();

    try {
      adminStmt.execute("CREATE ROLE role1");
      ResultSet resultSet = adminStmt.executeQuery("LIST ROLE PRIVILEGES role1");
      String ans = "";
      try {
        // not granted list role privilege, should return empty
        validateResultSet(resultSet, ans);

        adminStmt.execute(
            "GRANT ROLE role1 PRIVILEGES READ_TIMESERIES,INSERT_TIMESERIES,DELETE_TIMESERIES ON root.a.b.c");
        adminStmt.execute(
            "GRANT ROLE role1 PRIVILEGES READ_TIMESERIES,INSERT_TIMESERIES,DELETE_TIMESERIES ON root.d.b.c");
        resultSet = adminStmt.executeQuery("LIST ROLE PRIVILEGES role1");
        ans =
            "root.a.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES,\n"
                + "root.d.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES,\n";
        validateResultSet(resultSet, ans);

        resultSet = adminStmt.executeQuery("LIST PRIVILEGES ROLE role1 ON root.a.b.c");
        ans = "root.a.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES,\n";
        validateResultSet(resultSet, ans);

        adminStmt.execute(
            "REVOKE ROLE role1 PRIVILEGES INSERT_TIMESERIES,DELETE_TIMESERIES ON root.a.b.c");

        resultSet = adminStmt.executeQuery("LIST ROLE PRIVILEGES role1");
        ans =
            "root.a.b.c : READ_TIMESERIES,\n"
                + "root.d.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES,\n";
        validateResultSet(resultSet, ans);

        resultSet = adminStmt.executeQuery("LIST PRIVILEGES ROLE role1 ON root.a.b.c");
        ans = "root.a.b.c : READ_TIMESERIES,\n";
        validateResultSet(resultSet, ans);
      } finally {
        resultSet.close();
      }
    } finally {
      adminStmt.close();
      adminCon.close();
    }
  }

  @Test
  public void testListUserRoles() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection adminCon =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement adminStmt = adminCon.createStatement();

    try {
      adminStmt.execute("CREATE USER chenduxiu 'orange'");

      adminStmt.execute("CREATE ROLE xijing");
      adminStmt.execute("CREATE ROLE dalao");
      adminStmt.execute("CREATE ROLE shenshi");
      adminStmt.execute("CREATE ROLE zhazha");
      adminStmt.execute("CREATE ROLE hakase");

      adminStmt.execute("GRANT xijing TO chenduxiu");
      adminStmt.execute("GRANT dalao TO chenduxiu");
      adminStmt.execute("GRANT shenshi TO chenduxiu");
      adminStmt.execute("GRANT zhazha TO chenduxiu");
      adminStmt.execute("GRANT hakase TO chenduxiu");

      ResultSet resultSet = adminStmt.executeQuery("LIST ALL ROLE OF USER chenduxiu");
      String ans = "xijing,\n" + "dalao,\n" + "shenshi,\n" + "zhazha,\n" + "hakase,\n";
      try {
        validateResultSet(resultSet, ans);

        adminStmt.execute("REVOKE dalao FROM chenduxiu");
        adminStmt.execute("REVOKE hakase FROM chenduxiu");

        resultSet = adminStmt.executeQuery("LIST ALL ROLE OF USER chenduxiu");
        ans = "xijing,\n" + "shenshi,\n" + "zhazha,\n";
        validateResultSet(resultSet, ans);
      } finally {
        resultSet.close();
      }
    } finally {
      adminStmt.close();
      adminCon.close();
    }
  }

  @Test
  public void testListRoleUsers() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection adminCon =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement adminStmt = adminCon.createStatement();

    try {
      adminStmt.execute("CREATE ROLE dalao");
      adminStmt.execute("CREATE ROLE zhazha");

      String[] members = {
        "HighFly",
        "SunComparison",
        "Persistence",
        "GoodWoods",
        "HealthHonor",
        "GoldLuck",
        "DoubleLight",
        "Eastwards",
        "ScentEffusion",
        "Smart",
        "East",
        "DailySecurity",
        "Moon",
        "RayBud",
        "RiverSky"
      };

      for (int i = 0; i < members.length - 1; i++) {
        adminStmt.execute("CREATE USER " + members[i] + " '666666'");
        adminStmt.execute("GRANT dalao TO  " + members[i]);
      }
      adminStmt.execute("CREATE USER RiverSky '2333333'");
      adminStmt.execute("GRANT zhazha TO RiverSky");

      ResultSet resultSet = adminStmt.executeQuery("LIST ALL USER OF ROLE dalao");
      String ans =
          "DailySecurity,\n"
              + "DoubleLight,\n"
              + "East,\n"
              + "Eastwards,\n"
              + "GoldLuck,\n"
              + "GoodWoods,\n"
              + "HealthHonor,\n"
              + "HighFly,\n"
              + "Moon,\n"
              + "Persistence,\n"
              + "RayBud,\n"
              + "ScentEffusion,\n"
              + "Smart,\n"
              + "SunComparison,\n";
      try {
        validateResultSet(resultSet, ans);

        resultSet = adminStmt.executeQuery("LIST ALL USER OF ROLE zhazha");
        ans = "RiverSky,\n";
        validateResultSet(resultSet, ans);

        adminStmt.execute("REVOKE zhazha from RiverSky");
        resultSet = adminStmt.executeQuery("LIST ALL USER OF ROLE zhazha");
        ans = "";
        validateResultSet(resultSet, ans);
      } finally {
        resultSet.close();
      }

    } finally {
      adminStmt.close();
      adminCon.close();
    }
  }

  private void validateResultSet(ResultSet set, String ans) throws SQLException {
    try {
      StringBuilder builder = new StringBuilder();
      ResultSetMetaData metaData = set.getMetaData();
      int colNum = metaData.getColumnCount();
      while (set.next()) {
        for (int i = 1; i <= colNum; i++) {
          builder.append(set.getString(i)).append(",");
        }
        builder.append("\n");
      }
      assertEquals(ans, builder.toString());
    } finally {
      set.close();
    }
  }

  @Test
  public void testListUserPrivilege() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection adminCon =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement adminStmt = adminCon.createStatement();

    for (int i = 0; i < 10; i++) {
      adminStmt.execute("CREATE USER user" + i + " 'password" + i + "'");
    }

    adminStmt.execute("CREATE USER tempuser 'temppw'");

    try (Connection userCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement()) {
      try {
        // without list user privilege
        ResultSet resultSet = userStmt.executeQuery("LIST USER");
        String ans = "";
        validateResultSet(resultSet, ans);

        // with list user privilege
        adminStmt.execute("GRANT USER tempuser PRIVILEGES LIST_USER ON root");
        resultSet = userStmt.executeQuery("LIST USER");
        ans =
            "root,\n"
                + "tempuser,\n"
                + "user0,\n"
                + "user1,\n"
                + "user2,\n"
                + "user3,\n"
                + "user4,\n"
                + "user5,\n"
                + "user6,\n"
                + "user7,\n"
                + "user8,\n"
                + "user9,\n";
        validateResultSet(resultSet, ans);
      } finally {
        userStmt.close();
      }
    } finally {
      adminCon.close();
    }
  }

  @Test
  public void testExecuteBatchWithPrivilege() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw'");
      try (Connection userCon =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
          Statement userStatement = userCon.createStatement()) {
        userStatement.addBatch("CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT64");
        userStatement.addBatch("CREATE TIMESERIES root.sg2.d1.s1 WITH DATATYPE=INT64");
        try {
          userStatement.executeBatch();
        } catch (BatchUpdateException e) {
          assertEquals(
              System.lineSeparator()
                  + "No permissions for this operation CREATE_TIMESERIES for SQL: \"CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT64\""
                  + System.lineSeparator()
                  + "No permissions for this operation CREATE_TIMESERIES for SQL: \"CREATE TIMESERIES root.sg2.d1.s1 WITH DATATYPE=INT64\""
                  + System.lineSeparator(),
              e.getMessage());
        }
      }
    }
  }

  @Test
  public void testExecuteBatchWithPrivilege1() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection adminCon =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw'");
      adminStmt.execute("GRANT USER tempuser PRIVILEGES INSERT_TIMESERIES on root.sg1");

      try (Connection userCon =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "tempuser", "temppw");
          Statement userStatement = userCon.createStatement()) {
        userStatement.addBatch("insert into root.sg1.d1(timestamp,s1) values (1,1)");
        userStatement.addBatch("insert into root.sg2.d1(timestamp,s1) values (2,1)");
        userStatement.addBatch("insert into root.sg1.d1(timestamp,s2) values (3,1)");
        userStatement.addBatch("insert into root.sg2.d1(timestamp,s1) values (4,1)");
        try {
          userStatement.executeBatch();
        } catch (BatchUpdateException e) {
          System.out.println(e.getMessage());
          assertEquals(
              System.lineSeparator()
                  + "No permissions for this operation INSERT for SQL: \"insert into root.sg2.d1(timestamp,s1) values (2,1)\""
                  + System.lineSeparator()
                  + "No permissions for this operation INSERT for SQL: \"insert into root.sg2.d1(timestamp,s1) values (4,1)\""
                  + System.lineSeparator(),
              e.getMessage());
        }
      }
      ResultSet resultSet = adminStmt.executeQuery("select * from root.**");
      String[] expected = new String[] {"1, 1.0", "1, null", "3, null", "3, 1.0"};
      List<String> expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      List<String> result = new ArrayList<>();
      while (resultSet.next()) {
        result.add(resultSet.getString("Time") + ", " + resultSet.getString("root.sg1.d1.s1"));
        result.add(resultSet.getString("Time") + ", " + resultSet.getString("root.sg1.d1.s2"));
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));
    }
  }
}
