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

package org.apache.iotdb.db.it.auth;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.apache.iotdb.commons.auth.entity.User.INTERNAL_USER_END_ID;
import static org.apache.iotdb.db.audit.DNAuditLogger.PREFIX_PASSWORD_HISTORY;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** This is an example for integration test. */
@SuppressWarnings("EmptyTryBlock")
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAuthIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void allPrivilegesTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw123456'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
          Statement userStmt = userCon.createStatement()) {
        // 1. tempuser doesn't have any privilege
        Assert.assertThrows(SQLException.class, () -> userStmt.execute("CREATE DATABASE root.a"));
        Assert.assertThrows(
            SQLException.class,
            () ->
                userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (100, 100)"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT WRITE_SCHEMA ON root.a TO USER tempuser"));
        Assert.assertThrows(
            SQLException.class, () -> userStmt.execute("LIST PRIVILEGES OF USER root"));

        ResultSet resultSet = userStmt.executeQuery("LIST USER");
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("10000", resultSet.getString(1));
        Assert.assertEquals("tempuser", resultSet.getString(2));
        Assert.assertFalse(resultSet.next());

        resultSet = userStmt.executeQuery("LIST PRIVILEGES OF USER tempuser");
        Assert.assertFalse(resultSet.next());

        //  2. admin grant all privileges to user tempuser, So tempuser can do anything.
        adminStmt.execute("GRANT ALL ON root.** TO USER tempuser");

        userStmt.execute("CREATE DATABASE root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (100, 100)");
        userStmt.execute("SELECT * from root.a");

        // 4. Admin grant write_schema, read_schema on root.** to tempuser.
        adminStmt.execute("GRANT WRITE_SCHEMA ON root.** TO USER tempuser WITH GRANT OPTION");
        adminStmt.execute("GRANT READ_SCHEMA ON root.** TO USER tempuser WITH GRANT OPTION");

        // 5. tempuser can grant or revoke his write_schema and read_schema to others or himself.
        userStmt.execute("GRANT WRITE_SCHEMA ON root.t1 TO USER tempuser");
        userStmt.execute("GRANT READ_SCHEMA ON root.t1.t2 TO USER tempuser WITH GRANT OPTION");
        userStmt.execute("REVOKE WRITE_SCHEMA ON root.t1 FROM USER tempuser");
        // tempuser revoke his write_schema privilege
        userStmt.execute("REVOKE WRITE_SCHEMA ON root.** FROM USER tempuser");

        Assert.assertThrows(
            SQLException.class, () -> userStmt.execute("GRANT READ_DATA root.t1 to USER tempuser"));

        // 6. REVOKE ALL will be ok.
        adminStmt.execute("REVOKE ALL on root.** FROM USER tempuser");
        adminStmt.execute("GRANT ALL ON root.** TO USER tempuser");
        adminStmt.execute("REVOKE ALL ON root.** FROM USER tempuser");

        Assert.assertThrows(SQLException.class, () -> userStmt.execute("CREATE DATABASE root.b"));

        // 7. With "WRITE" privilege, tempuser can read,write schema or data.
        adminStmt.execute("GRANT WRITE, SYSTEM on root.** TO USER tempuser");
        userStmt.execute("CREATE DATABASE root.c");
        userStmt.execute("CREATE TIMESERIES root.c.d WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.c(timestamp, d) VALUES (100, 100)");
        ResultSet result = userStmt.executeQuery("SELECT * from root.c");
        String ans = "100,100,\n";
        validateResultSet(result, ans);

        adminStmt.execute("REVOKE WRITE, SYSTEM on root.** FROM USER tempuser");
        adminStmt.execute("GRANT READ on root.** TO USER tempuser");

        result = userStmt.executeQuery("SELECT * from root.c");
        ans = "100,100,\n";
        validateResultSet(result, ans);

        adminStmt.execute("REVOKE READ on root.** FROM USER tempuser");

        Assert.assertThrows(SQLException.class, () -> userStmt.execute("CREATE DATABASE root.b"));
        Assert.assertThrows(
            SQLException.class,
            () ->
                userStmt.execute("CREATE TIMESERIES root.b.b WITH DATATYPE=INT32,ENCODING=PLAIN"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("INSERT INTO root.b(timestamp, b) VALUES (100, 100)"));
        result = userStmt.executeQuery("SELECT * from root.a");
        validateResultSet(result, "");
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT USER tempuser PRIVILEGES WRITE_SCHEMA ON root.a"));

        adminStmt.execute("GRANT ALL ON root.** TO USER tempuser WITH GRANT OPTION");
        userStmt.execute("CREATE USER testuser 'password123456'");
        userStmt.execute("GRANT ALL ON root.** TO USER testuser WITH GRANT OPTION");
        ResultSet dataSet = userStmt.executeQuery("LIST PRIVILEGES OF USER testuser");

        Set<String> ansSet =
            new HashSet<>(
                Arrays.asList(
                    ",,SYSTEM,true,",
                    ",,SECURITY,true,",
                    ",root.**,READ_DATA,true,",
                    ",root.**,WRITE_DATA,true,",
                    ",root.**,READ_SCHEMA,true,",
                    ",root.**,WRITE_SCHEMA,true,"));
        TestUtils.assertResultSetEqual(dataSet, "Role,Scope,Privileges,GrantOption,", ansSet);
      }
    }
  }

  @Test
  public void testSetDeleteSG() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER sgtest 'sgtest123456'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("sgtest", "sgtest123456");
          Statement userStmt = userCon.createStatement()) {

        Assert.assertThrows(
            SQLException.class, () -> userStmt.execute("CREATE DATABASE root.sgtest"));

        adminStmt.execute("GRANT SYSTEM ON root.** TO USER sgtest");

        try {
          userStmt.execute("CREATE DATABASE root.sgtest");
          userStmt.execute("DELETE DATABASE root.sgtest");
        } catch (SQLException e) {
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void illegalPasswordTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      Assert.assertThrows(
          SQLException.class, () -> adminStmt.execute("CREATE USER tempuser 'temppw 123456'"));
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      Assert.assertThrows(SQLException.class, () -> adminStmt.execute("CREATE USER tempuser 'te'"));
    }
  }

  @Test
  public void updatePasswordTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw123456'");
      Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
      userCon.close();

      adminStmt.execute("ALTER USER tempuser SET PASSWORD 'newpw1234567'");

      boolean caught = false;
      try {
        userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw1234567");
      } catch (SQLException e) {
        caught = true;
      } finally {
        userCon.close();
      }
      assertTrue(caught);

      userCon = EnvFactory.getEnv().getConnection("tempuser", "newpw1234567");

      userCon.close();
    }
  }

  @Test
  public void illegalGrantRevokeUserTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw123456'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
          Statement userStmt = userCon.createStatement()) {

        // grant a non-existing user
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("GRANT WRITE_SCHEMA on root.a TO USER 'not a user'"));
        // grant a non-existing privilege
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("GRANT NOT_A_PRIVILEGE on root.a TO USER tempuser"));
        adminStmt.execute("GRANT SECURITY on root.** TO USER tempuser");
        // grant on an illegal seriesPath
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("GRANT WRITE_SCHEMA on a.b TO USER tempuser"));
        // grant admin
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("GRANT WRITE_SCHEMA on root.a.b TO USER root"));
        // revoke a non-existing privilege
        adminStmt.execute("REVOKE SECURITY on root.** FROM USER tempuser");

        // revoke a non-existing user
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("REVOKE SECURITY on root.** FROM USER tempuser1"));
        // revoke on an illegal seriesPath
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("REVOKE WRITE_SCHEMA on a.b FROM USER tempuser"));
        // revoke admin
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("REVOKE WRITE_SCHEMA on root.a.b FROM USER root"));
        // no privilege to revoke
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("REVOKE WRITE_SCHEMA on root.a.b FROM USER tempuser"));
        // grant privilege to grant
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT WRITE_SCHEMA on root.a.b TO USER tempuser"));
        adminStmt.execute("GRANT WRITE_SCHEMA ON root.** TO USER tempuser WITH GRANT OPTION");
        userStmt.execute("GRANT WRITE_SCHEMA on root.** TO USER tempuser");
        userStmt.execute("REVOKE WRITE_SCHEMA on root.** FROM USER tempuser");
      }
    }
  }

  @Test
  public void createDeleteTimeSeriesTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {

      adminStmt.execute("CREATE USER tempuser 'temppw123456'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
          Statement userStmt = userCon.createStatement()) {

        // grant and revoke the user the privilege to create time series
        Assert.assertThrows(SQLException.class, () -> userStmt.execute("CREATE DATABASE root.a"));

        adminStmt.execute("GRANT SYSTEM,WRITE_SCHEMA ON root.** TO USER tempuser");
        userStmt.execute("CREATE DATABASE root.a");
        adminStmt.execute("GRANT WRITE_SCHEMA ON root.a.b TO USER tempuser");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("CREATE DATABASE root.b");
        // grant again wil success.
        adminStmt.execute("GRANT SYSTEM,WRITE_SCHEMA ON root.** TO USER tempuser");

        adminStmt.execute("REVOKE SYSTEM,WRITE_SCHEMA ON root.** FROM USER tempuser");
        // no privilege to create this one anymore
        Assert.assertThrows(
            SQLException.class,
            () ->
                userStmt.execute("CREATE TIMESERIES root.b.a WITH DATATYPE=INT32,ENCODING=PLAIN"));

        adminStmt.execute("REVOKE WRITE_SCHEMA ON root.a.b FROM USER tempuser");
        // no privilege to create this one anymore
        Assert.assertThrows(
            SQLException.class,
            () ->
                userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN"));
      }
    }
  }

  @Test
  public void templateQueryTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw123456'");
      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
          Statement userStmt = userCon.createStatement()) {
        adminStmt.execute(
            "GRANT READ_DATA ON root.sg.aligned_template.temperature TO USER tempuser");
        adminStmt.execute("CREATE DATABASE root.sg");
        adminStmt.execute(
            "create device template t1 aligned (temperature FLOAT encoding=Gorilla, status BOOLEAN encoding=PLAIN);");
        adminStmt.execute("set device template t1 to root.sg.aligned_template;");
        adminStmt.execute("create timeseries using device template on root.sg.aligned_template;");
        adminStmt.execute(
            "insert into root.sg.aligned_template(time,temperature,status) values(1,20,false),(2,22.1,true),(3,18,false);");

        ResultSet set1 = adminStmt.executeQuery("SELECT * from root.sg.aligned_template");
        assertEquals(3, set1.getMetaData().getColumnCount());
        assertEquals("root.sg.aligned_template.temperature", set1.getMetaData().getColumnName(2));
        assertEquals("root.sg.aligned_template.status", set1.getMetaData().getColumnName(3));

        ResultSet set2 = userStmt.executeQuery("SELECT * from root.sg.aligned_template");
        assertEquals(2, set2.getMetaData().getColumnCount());
        assertEquals("root.sg.aligned_template.temperature", set2.getMetaData().getColumnName(2));
      }
    }
  }

  @Test
  public void insertQueryTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw123456'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
          Statement userStmt = userCon.createStatement()) {

        adminStmt.execute("GRANT SYSTEM ON root.** TO USER tempuser");
        userStmt.execute("CREATE DATABASE root.a");
        adminStmt.execute("GRANT WRITE_SCHEMA ON root.a.b TO USER tempuser");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");

        // grant privilege to insert
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)"));

        adminStmt.execute("GRANT WRITE_DATA on root.a.** TO USER tempuser");
        userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");

        // revoke privilege to insert
        adminStmt.execute("REVOKE WRITE_DATA on root.a.** FROM USER tempuser");
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)"));
        // grant privilege to query
        ResultSet set = userStmt.executeQuery("SELECT * from root.a");
        assertFalse(set.next());

        adminStmt.execute("GRANT READ_DATA on root.** TO USER tempuser");
        ResultSet resultSet = userStmt.executeQuery("SELECT * from root.a");
        resultSet.close();
        resultSet = userStmt.executeQuery("SELECT LAST b from root.a");
        resultSet.close();

        // revoke privilege to query
        adminStmt.execute("REVOKE READ_DATA on root.** FROM USER tempuser");
        // get empty result when the user don't have privilege
        ResultSet set2 = userStmt.executeQuery("SELECT * from root.a");
        assertFalse(set2.next());
      }
    }
  }

  @Test
  public void rolePrivilegeTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {

      adminStmt.execute("CREATE USER tempuser 'temppw123456'");
      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
          Statement userStmt = userCon.createStatement()) {

        Assert.assertThrows(SQLException.class, () -> userStmt.execute("CREATE ROLE admin"));

        adminStmt.execute("CREATE ROLE admin");
        adminStmt.execute("GRANT SYSTEM,WRITE_SCHEMA,WRITE_DATA on root.** TO ROLE admin");
        adminStmt.execute("GRANT ROLE admin TO tempuser");
        adminStmt.execute("CREATE ROLE admin_temp");

        // tempuser can get privileges of his role
        userStmt.execute("LIST PRIVILEGES OF ROLE admin");
        Assert.assertThrows(
            SQLException.class, () -> userStmt.execute("LIST PRIVILEGS OF ROLE admin_temp"));

        userStmt.execute("CREATE DATABASE root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("CREATE TIMESERIES root.a.c WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.a(timestamp,b,c) VALUES (1,100,1000)");
        // userStmt.execute("DELETE FROM root.a.b WHERE TIME <= 1000000000");
        ResultSet resultSet = userStmt.executeQuery("SELECT * FROM root.a");
        validateResultSet(resultSet, "1,100,1000,\n");
        resultSet.close();

        adminStmt.execute("REVOKE SYSTEM,WRITE_SCHEMA on root.** FROM ROLE admin");
        adminStmt.execute("GRANT READ_DATA on root.** TO USER tempuser");
        adminStmt.execute("REVOKE ROLE admin FROM tempuser");
        resultSet = userStmt.executeQuery("SELECT * FROM root.**");
        resultSet.close();

        Assert.assertThrows(
            SQLException.class,
            () ->
                userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN"));
      }
    }
  }

  @Test
  public void testListUser() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();

    try {
      ResultSet resultSet = adminStmt.executeQuery("LIST USER");
      String ans = "0,root,\n";
      try {
        validateResultSet(resultSet, ans);

        for (int i = 0; i < 10; i++) {
          adminStmt.execute("CREATE USER user" + i + " 'password" + i + "123456'");
        }
        resultSet = adminStmt.executeQuery("LIST USER");
        ans =
            "0,root,\n"
                + "10000,user0,\n"
                + "10001,user1,\n"
                + "10002,user2,\n"
                + "10003,user3,\n"
                + "10004,user4,\n"
                + "10005,user5,\n"
                + "10006,user6,\n"
                + "10007,user7,\n"
                + "10008,user8,\n"
                + "10009,user9,\n";
        validateResultSet(resultSet, ans);

        for (int i = 0; i < 10; i++) {
          if (i % 2 == 0) {
            adminStmt.execute("DROP USER user" + i);
          }
        }
        resultSet = adminStmt.executeQuery("LIST USER");
        ans =
            "0,root,\n"
                + "10001,user1,\n"
                + "10003,user3,\n"
                + "10005,user5,\n"
                + "10007,user7,\n"
                + "10009,user9,\n";
        validateResultSet(resultSet, ans);
      } finally {
        resultSet.close();
      }
    } finally {
      adminCon.close();
    }
  }

  @Test
  public void testListRole() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection();
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
  public void testListUserRole() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();
    try {
      adminStmt.execute("CREATE USER user1 'password123456'");
      adminStmt.execute("CREATE USER user2 'password123456'");
      adminStmt.execute("CREATE ROLE role1");
      adminStmt.execute("CREATE ROLE role2");
      adminStmt.execute("GRANT ROLE role1 TO user1");
      adminStmt.execute("GRANT ROLE role1 TO user2");
      adminStmt.execute("GRANT ROLE role2 TO user2");
      adminStmt.execute("GRANT SECURITY ON root.** TO USER user1");

      // user1 : role1; MANAGE_ROLE,MANAGE_USER
      // user2 : role1, role2;
      ResultSet resultSet;
      String ans;
      Connection userCon = EnvFactory.getEnv().getConnection("user1", "password123456");
      Statement userStmt = userCon.createStatement();
      try {
        resultSet = userStmt.executeQuery("LIST ROLE OF USER user1");
        ans = "role1,\n";
        validateResultSet(resultSet, ans);
        resultSet = userStmt.executeQuery("LIST ROLE OF USER user2");
        ans = "role1,\nrole2,\n";
        validateResultSet(resultSet, ans);
        resultSet = userStmt.executeQuery("LIST USER OF ROLE role1");
        ans = "10000,user1,\n10001,user2,\n";
        validateResultSet(resultSet, ans);
      } finally {
        userStmt.close();
        userCon.close();
      }

      Connection user2Con = EnvFactory.getEnv().getConnection("user2", "password123456");
      Statement user2Stmt = user2Con.createStatement();
      try {
        Assert.assertThrows(SQLException.class, () -> user2Stmt.execute("LIST ROLE OF USER user1"));
        Assert.assertThrows(SQLException.class, () -> user2Stmt.execute("LIST USER OF ROLE user1"));
        resultSet = user2Stmt.executeQuery("LIST ROLE OF USER user2");
        ans = "role1,\nrole2,\n";
        validateResultSet(resultSet, ans);
      } finally {
        user2Stmt.close();
        user2Con.close();
      }
    } finally {
      adminStmt.close();
      adminCon.close();
    }
  }

  @Test
  public void testListUserPrivileges() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();

    try {
      adminStmt.execute("CREATE USER user1 'password123456'");
      adminStmt.execute("GRANT READ_SCHEMA ON root.a.b TO USER user1");
      adminStmt.execute("CREATE ROLE role1");
      adminStmt.execute("GRANT READ_SCHEMA,WRITE_DATA ON root.a.b.c TO ROLE role1");
      adminStmt.execute(
          "GRANT READ_SCHEMA,WRITE_DATA ON root.d.b.c TO ROLE role1 WITH GRANT OPTION");
      adminStmt.execute("GRANT ROLE role1 TO user1");

      ResultSet resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF USER user1");
      String ans =
          ",root.a.b,READ_SCHEMA,false,\n"
              + "role1,root.a.b.c,WRITE_DATA,false,\n"
              + "role1,root.a.b.c,READ_SCHEMA,false,\n"
              + "role1,root.d.b.c,READ_SCHEMA,true,\n"
              + "role1,root.d.b.c,WRITE_DATA,true,\n";
      try {
        validateResultSet(resultSet, ans);

        adminStmt.execute("REVOKE ROLE role1 from user1");

        resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF USER user1");
        ans = ",root.a.b,READ_SCHEMA,false,\n";
        validateResultSet(resultSet, ans);
        resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF USER root");
        ans =
            ",,SYSTEM,true,\n"
                + ",,SECURITY,true,\n"
                + ",root.**,READ_DATA,true,\n"
                + ",root.**,WRITE_DATA,true,\n"
                + ",root.**,READ_SCHEMA,true,\n"
                + ",root.**,WRITE_SCHEMA,true,\n";
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
  public void testListRolePrivileges() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();

    try {
      adminStmt.execute("CREATE ROLE role1");
      ResultSet resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF ROLE role1");
      String ans = "";
      try {
        // not granted list role privilege, should return empty
        validateResultSet(resultSet, ans);

        adminStmt.execute("GRANT READ_SCHEMA,WRITE_DATA ON root.a.b.c TO ROLE role1");
        adminStmt.execute(
            "GRANT READ_SCHEMA,WRITE_DATA ON root.d.b.c TO ROLE role1 WITH GRANT OPTION");
        resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF ROLE role1");
        ans =
            "role1,root.a.b.c,WRITE_DATA,false,\nrole1,root.a.b.c,READ_SCHEMA,false,\nrole1,root.d.b.c,READ_SCHEMA,true,\nrole1,root.d.b.c,WRITE_DATA,true,\n";
        validateResultSet(resultSet, ans);

        adminStmt.execute("REVOKE READ_SCHEMA,WRITE_DATA ON root.a.b.c FROM ROLE role1");

        resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF ROLE role1");
        ans = "role1,root.d.b.c,WRITE_DATA,true,\n" + "role1,root.d.b.c,READ_SCHEMA,true,\n";
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
  public void testListUserRoles() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();

    try {
      adminStmt.execute("CREATE USER chenduxiu 'orange123456'");

      adminStmt.execute("CREATE ROLE xijing");
      adminStmt.execute("CREATE ROLE dalao");
      adminStmt.execute("CREATE ROLE shenshi");
      adminStmt.execute("CREATE ROLE zhazha");
      adminStmt.execute("CREATE ROLE hakase");

      adminStmt.execute("GRANT ROLE xijing TO chenduxiu");
      adminStmt.execute("GRANT ROLE dalao TO chenduxiu");
      adminStmt.execute("GRANT ROLE shenshi TO chenduxiu");
      adminStmt.execute("GRANT ROLE zhazha TO chenduxiu");
      adminStmt.execute("GRANT ROLE hakase TO chenduxiu");

      ResultSet resultSet = adminStmt.executeQuery("LIST ROLE OF USER chenduxiu");
      String ans = "xijing,\n" + "dalao,\n" + "shenshi,\n" + "zhazha,\n" + "hakase,\n";
      try {
        validateResultSet(resultSet, ans);

        adminStmt.execute("REVOKE ROLE dalao FROM chenduxiu");
        adminStmt.execute("REVOKE ROLE hakase FROM chenduxiu");

        resultSet = adminStmt.executeQuery("LIST ROLE OF USER chenduxiu");
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
  public void testListRoleUsers() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection();
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
        adminStmt.execute("CREATE USER " + members[i] + " 'a666666123456'");
        adminStmt.execute("GRANT ROLE dalao TO  " + members[i]);
      }
      adminStmt.execute("CREATE USER RiverSky 'a2333333123456'");
      adminStmt.execute("GRANT ROLE zhazha TO RiverSky");

      ResultSet resultSet = adminStmt.executeQuery("LIST USER OF ROLE dalao");
      String ans =
          "10011,DailySecurity,\n"
              + "10006,DoubleLight,\n"
              + "10010,East,\n"
              + "10007,Eastwards,\n"
              + "10005,GoldLuck,\n"
              + "10003,GoodWoods,\n"
              + "10004,HealthHonor,\n"
              + "10000,HighFly,\n"
              + "10012,Moon,\n"
              + "10002,Persistence,\n"
              + "10013,RayBud,\n"
              + "10008,ScentEffusion,\n"
              + "10009,Smart,\n"
              + "10001,SunComparison,\n";
      try {
        validateResultSet(resultSet, ans);

        resultSet = adminStmt.executeQuery("LIST USER OF ROLE zhazha");
        ans = "10014,RiverSky,\n";
        validateResultSet(resultSet, ans);

        adminStmt.execute("REVOKE ROLE zhazha from RiverSky");
        resultSet = adminStmt.executeQuery("LIST USER OF ROLE zhazha");
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

  public static void validateResultSet(ResultSet set, String ans) throws SQLException {
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
      String result = builder.toString();
      assertEquals(ans.length(), result.length());
      List<String> ansLines = Arrays.asList(ans.split("\n"));
      List<String> resultLines = Arrays.asList(result.split("\n"));
      assertEquals(ansLines.size(), resultLines.size());
      for (String resultLine : resultLines) {
        assertTrue(ansLines.contains(resultLine));
      }
    } finally {
      set.close();
    }
  }

  @Test
  public void testListUserPrivilege() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();

    for (int i = 0; i < 10; i++) {
      adminStmt.execute("CREATE USER user" + i + " 'password" + i + "123456'");
    }

    adminStmt.execute("CREATE USER tempuser 'temppw123456'");

    try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
        Statement userStmt = userCon.createStatement()) {
      try {
        String ans = "10010,tempuser,\n";
        ResultSet resultSet = userStmt.executeQuery("LIST USER");
        validateResultSet(resultSet, ans);
        // with list user privilege
        adminStmt.execute("GRANT SECURITY on root.** TO USER tempuser");
        resultSet = userStmt.executeQuery("LIST USER");
        ans =
            "0,root,\n"
                + "10010,tempuser,\n"
                + "10000,user0,\n"
                + "10001,user1,\n"
                + "10002,user2,\n"
                + "10003,user3,\n"
                + "10004,user4,\n"
                + "10005,user5,\n"
                + "10006,user6,\n"
                + "10007,user7,\n"
                + "10008,user8,\n"
                + "10009,user9,\n";
        validateResultSet(resultSet, ans);
      } finally {
        userStmt.close();
      }
    } finally {
      adminCon.close();
    }
  }

  @Test
  public void testExecuteBatchWithPrivilege() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection("root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw123456'");
      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
          Statement userStatement = userCon.createStatement()) {
        userStatement.addBatch("CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT64");
        userStatement.addBatch("CREATE TIMESERIES root.sg2.d1.s1 WITH DATATYPE=INT64");
        Assert.assertThrows(BatchUpdateException.class, () -> userStatement.executeBatch());
      }
    }
  }

  @Test
  public void testExecuteBatchWithPrivilege1() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw123456'");
      adminStmt.execute("GRANT WRITE_DATA on root.sg1.** TO USER tempuser");
      adminStmt.execute("GRANT WRITE_SCHEMA on root.sg1.** TO USER tempuser");
      adminStmt.execute("GRANT SYSTEM on root.** TO USER tempuser");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
          Statement userStatement = userCon.createStatement()) {
        userStatement.addBatch("insert into root.sg1.d1(timestamp,s1) values (1,1)");
        userStatement.addBatch("insert into root.sg1.d1(timestamp,s2) values (3,1)");
        userStatement.addBatch("insert into root.sg2.d1(timestamp,s1) values (2,1)");
        userStatement.addBatch("insert into root.sg2.d1(timestamp,s1) values (4,1)");
        Assert.assertThrows(BatchUpdateException.class, userStatement::executeBatch);
      }
      ResultSet resultSet = adminStmt.executeQuery("select * from root.sg1.**");
      String[] expected = new String[] {"1, 1.0", "1, null", "3, null", "3, 1.0"};
      List<String> expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      List<String> result = new ArrayList<>();
      while (resultSet.next()) {
        result.add(
            resultSet.getString(ColumnHeaderConstant.TIME)
                + ", "
                + resultSet.getString("root.sg1.d1.s1"));
        result.add(
            resultSet.getString(ColumnHeaderConstant.TIME)
                + ", "
                + resultSet.getString("root.sg1.d1.s2"));
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));
    }
  }

  /** ISSUE-4308 */
  @Test
  public void testSelectUDTF() throws SQLException {
    try (Connection adminConnection = EnvFactory.getEnv().getConnection();
        Statement adminStatement = adminConnection.createStatement()) {
      adminStatement.execute("CREATE USER a_application 'a_application123456'");
      adminStatement.execute("CREATE ROLE application_role");
      adminStatement.execute("GRANT READ_DATA ON root.test.** TO ROLE application_role");
      adminStatement.execute("GRANT ROLE application_role TO a_application");

      adminStatement.execute("INSERT INTO root.test(time, s1, s2, s3) VALUES(1, 2, 3, 4)");
    }

    try (Connection userConnection =
            EnvFactory.getEnv().getConnection("a_application", "a_application123456");
        Statement userStatement = userConnection.createStatement();
        ResultSet resultSet =
            userStatement.executeQuery(
                "SELECT s1, s1, s1 - s3, s2 * sin(s1), s1 + 1 / 2 * sin(s1), sin(s1), sin(s1) FROM root.test")) {
      assertTrue(resultSet.next());
    }
  }

  /** IOTDB-2769 */
  @Test
  public void testGrantUserRole() throws SQLException {
    try (Connection adminConnection = EnvFactory.getEnv().getConnection();
        Statement adminStatement = adminConnection.createStatement()) {
      adminStatement.execute("CREATE USER user01 'pass1234123456'");
      adminStatement.execute("CREATE USER user02 'pass1234123456'");
      adminStatement.execute("CREATE ROLE manager");
      adminStatement.execute("GRANT SECURITY on root.** TO USER user01");
      Assert.assertThrows(
          SQLException.class, () -> adminStatement.execute("GRANT role manager to `root`"));
    }

    try (Connection userCon = EnvFactory.getEnv().getConnection("user01", "pass1234123456");
        Statement userStatement = userCon.createStatement()) {
      try {
        userStatement.execute("grant role manager to user02");
      } catch (SQLException e) {
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void testGrantAndGrantOpt() throws SQLException {
    // 1. CREATE USER1, USER2, USER3, testRole
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();
    adminStmt.execute("CREATE USER user1 'password123456'");
    adminStmt.execute("CREATE USER user2 'password123456'");
    adminStmt.execute("CREATE USER user3 'password123456'");
    adminStmt.execute("CREATE USER user4 'password123456'");
    adminStmt.execute("CREATE USER user5 'password123456'");
    adminStmt.execute("CREATE ROLE testRole");
    adminStmt.execute("GRANT system ON root.** TO ROLE testRole WITH GRANT OPTION");
    adminStmt.execute("GRANT READ_DATA ON root.t1.** TO ROLE testRole");
    adminStmt.execute("GRANT READ_SCHEMA ON root.t3.t2.** TO ROLE testRole WITH GRANT OPTION");

    // 2. USER1 has all privileges on root.**
    for (PrivilegeType item : PrivilegeType.values()) {
      if (item.isDeprecated() || item.isHided()) {
        continue;
      }
      if (item.isRelationalPrivilege()) {
        continue;
      }
      String sql = "GRANT %s on root.** to USER user1";
      adminStmt.execute(String.format(sql, item));
    }
    // 3.admin lists privileges of user1
    ResultSet resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF USER user1");
    String ans =
        ",,SYSTEM,false,\n"
            + ",,SECURITY,false,\n"
            + ",root.**,READ_DATA,false,\n"
            + ",root.**,WRITE_DATA,false,\n"
            + ",root.**,READ_SCHEMA,false,\n"
            + ",root.**,WRITE_SCHEMA,false,\n";
    validateResultSet(resultSet, ans);

    // 4. USER2 has all privilegs on root.** with grant option;
    for (PrivilegeType item : PrivilegeType.values()) {
      if (item.isRelationalPrivilege() || item.isDeprecated() || item.isHided()) {
        continue;
      }
      String sql = "GRANT %s on root.** to USER user2 with grant option";
      adminStmt.execute(String.format(sql, item));
    }
    resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF USER user2");
    ans =
        ",,SYSTEM,true,\n"
            + ",,SECURITY,true,\n"
            + ",root.**,READ_DATA,true,\n"
            + ",root.**,WRITE_DATA,true,\n"
            + ",root.**,READ_SCHEMA,true,\n"
            + ",root.**,WRITE_SCHEMA,true,\n";
    validateResultSet(resultSet, ans);

    // now user1 has all privileges, user2 has all privileges with grant option, user3 doesn't have
    // privileges

    // 5. Login user1 to list user2 privileges will success
    // user1 cannot grant any privilegs to user3
    try (Connection userCon = EnvFactory.getEnv().getConnection("user1", "password123456");
        Statement userStmt = userCon.createStatement()) {
      try {
        resultSet = userStmt.executeQuery("LIST PRIVILEGES OF USER user1");
        ans =
            ",,SYSTEM,false,\n"
                + ",,SECURITY,false,\n"
                + ",root.**,READ_DATA,false,\n"
                + ",root.**,WRITE_DATA,false,\n"
                + ",root.**,READ_SCHEMA,false,\n"
                + ",root.**,WRITE_SCHEMA,false,\n";
        validateResultSet(resultSet, ans);
      } finally {
        userStmt.close();
      }
    }
    // 6.Login user2 grant and revoke will success.
    try (Connection userCon = EnvFactory.getEnv().getConnection("user2", "password123456");
        Statement userStmt = userCon.createStatement()) {
      try {
        resultSet = userStmt.executeQuery("LIST PRIVILEGES OF USER user1");
        validateResultSet(resultSet, ans);
        userStmt.execute("GRANT SECURITY ON root.** TO USER user3");
        resultSet = userStmt.executeQuery("LIST PRIVILEGES OF USER user3");
        ans = ",,SECURITY,false,\n";
        validateResultSet(resultSet, ans);

        userStmt.execute("REVOKE SECURITY ON root.** FROM USER user1");
        resultSet = userStmt.executeQuery("LIST PRIVILEGES OF USER user1");
        ans =
            ",,SYSTEM,false,\n"
                + ",root.**,READ_DATA,false,\n"
                + ",root.**,WRITE_DATA,false,\n"
                + ",root.**,READ_SCHEMA,false,\n"
                + ",root.**,WRITE_SCHEMA,false,\n";
        validateResultSet(resultSet, ans);
      } finally {
        userStmt.close();
      }
    }
    adminStmt.execute("GRANT ROLE testRole TO user3");
    adminStmt.execute("REVOKE SECURITY ON root.** FROM USER user3");
    // now user has:
    // 1. MANAGE_ROLE
    // 2. MANAGE_DATABASE with grant option
    // 3. READ_DATA on root.t1.**
    // 4. READ_SCHEMA on root.t3.t2.**

    try (Connection userCon = EnvFactory.getEnv().getConnection("user3", "password123456");
        Statement userStmt = userCon.createStatement()) {
      try {
        // because role's privilege
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT READ_DATA ON root.t1.** TO USER user1"));
        userStmt.execute("GRANT READ_SCHEMA ON root.t3.t2.t3 TO USER user1");
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT READ_DATA ON root.t1.t2.t3 TO USER user1"));
      } finally {
        userStmt.close();
      }
    }

    try (Connection userCon = EnvFactory.getEnv().getConnection("user4", "password123456");
        Statement userStmt = userCon.createStatement()) {
      adminStmt.execute("GRANT SYSTEM ON root.** TO USER user4");
      try {
        Assert.assertThrows(
            SQLException.class, () -> userStmt.execute("GRANT SYSTEM ON root.** TO USER user5"));
        adminStmt.execute("GRANT SYSTEM ON root.** TO USER user5");
      } finally {
        userStmt.close();
      }
    }

    adminStmt.close();
  }

  @Test
  public void testRevokeAndGrantOpt() throws SQLException {
    // 1. revoke from user/role
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();
    adminStmt.execute("CREATE USER user1 'password123456'");
    adminStmt.execute("CREATE USER user2 'password123456'");

    // 2. user1 has all privileges with grant option on root.**
    //    user2 has all privileges without grant option on root.**
    //    user2 has all privileges without grant option on root.t1.**
    for (PrivilegeType item : PrivilegeType.values()) {
      if (item.isRelationalPrivilege() || item.isDeprecated() || item.isHided()) {
        continue;
      }
      String sql = "GRANT %s on root.** to USER user1 WITH GRANT OPTION";
      adminStmt.execute(String.format(sql, item));
      if (item.isPathPrivilege()) {
        adminStmt.execute(String.format("GRANT %s on root.t1.** TO USER user2", item));
      }
      sql = "GRANT %s on root.** to USER user2";
      adminStmt.execute(String.format(sql, item));
    }
    Connection user1Con = EnvFactory.getEnv().getConnection("user1", "password123456");
    Statement user1Stmt = user1Con.createStatement();
    ResultSet resultSet;
    String ans = "";
    try {
      // revoke privileges on root.** and root.t1.**
      for (PrivilegeType item : PrivilegeType.values()) {
        if (item.isRelationalPrivilege() || item.isHided() || item.isDeprecated()) {
          continue;
        }
        user1Stmt.execute(String.format("REVOKE %s ON root.** FROM USER user2", item));
      }

      resultSet = user1Stmt.executeQuery("LIST PRIVILEGES OF USER user2");
      validateResultSet(resultSet, ans);
    } finally {
      user1Stmt.close();
      user1Con.close();
    }
    adminStmt.execute("REVOKE ALL ON root.** from USER user1");
    resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF USER user1");
    validateResultSet(resultSet, ans);
    adminStmt.execute("GRANT READ,WRITE on root.t1.** TO USER user1");
    adminStmt.execute("REVOKE WRITE on root.** FROM USER user1");
    resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF USER user1");
    ans = ",root.t1.**,READ_DATA,false,\n" + ",root.t1.**,READ_SCHEMA,false,\n";
    validateResultSet(resultSet, ans);
  }

  @Test
  public void testQueryTemplate() throws SQLException {
    // 1. revoke from user/role
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();
    adminStmt.execute("CREATE USER user1 'password123456'");
    adminStmt.execute("GRANT READ_DATA ON root.sg.d1.** TO USER user1 with grant option;");
    adminStmt.execute("GRANT READ_DATA ON root.sg.aligned_template.temperature TO USER user1;");
    adminStmt.execute("CREATE DATABASE root.sg;");
    adminStmt.execute(
        "create device template t1 aligned (temperature FLOAT encoding=Gorilla, status BOOLEAN encoding=PLAIN);");
    adminStmt.execute("set device template t1 to root.sg.aligned_template;");
    adminStmt.execute("insert into root.sg.d1(time,s1,s2) values(1,1,1)");
    adminStmt.execute("insert into root.sg.d2(time,s1,s2) values(1,1,1)");
    adminStmt.execute(
        "insert into root.sg.aligned_template(time,temperature,status) values(1,20,true)");
    try (ResultSet resultSet = adminStmt.executeQuery("select * from root.sg.**;")) {
      Set<String> standards =
          new HashSet<>(
              Arrays.asList(
                  "Time",
                  "root.sg.aligned_template.temperature",
                  "root.sg.aligned_template.status",
                  "root.sg.d2.s1",
                  "root.sg.d2.s2",
                  "root.sg.d1.s1",
                  "root.sg.d1.s2"));
      ResultSetMetaData metaData = resultSet.getMetaData();
      for (int i = 1; i < metaData.getColumnCount() + 1; i++) {
        Assert.assertTrue(standards.remove(metaData.getColumnName(i)));
      }
      Assert.assertTrue(standards.isEmpty());
    }
    Connection user1Con = EnvFactory.getEnv().getConnection("user1", "password123456");
    Statement user1Stmt = user1Con.createStatement();
    try (ResultSet resultSet = user1Stmt.executeQuery("select * from root.**;")) {
      Set<String> standards =
          new HashSet<>(
              Arrays.asList(
                  "Time",
                  "root.sg.aligned_template.temperature",
                  "root.sg.d1.s1",
                  "root.sg.d1.s2"));
      ResultSetMetaData metaData = resultSet.getMetaData();
      for (int i = 1; i < metaData.getColumnCount() + 1; i++) {
        Assert.assertTrue(standards.remove(metaData.getColumnName(i)));
      }
      Assert.assertTrue(standards.isEmpty());
    }
  }

  @Test
  public void insertWithTemplateTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw123456'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw123456");
          Statement userStmt = userCon.createStatement()) {

        adminStmt.execute("CREATE DATABASE root.a");
        adminStmt.execute("create schema template t1 aligned (s_name TEXT)");
        adminStmt.execute("GRANT SYSTEM ON root.** TO USER tempuser");
        adminStmt.execute("GRANT WRITE_DATA ON root.a.** TO USER tempuser");
        adminStmt.execute("set schema template t1 to root.a");

        // grant privilege to insert
        Assert.assertThrows(
            SQLException.class,
            () ->
                userStmt.execute(
                    "INSERT INTO root.a.d1(timestamp, s_name, s_value) VALUES (1,'IoTDB', 2)"));

        adminStmt.execute("GRANT WRITE_SCHEMA ON root.a.d1.** TO USER tempuser");
        userStmt.execute("INSERT INTO root.a.d1(timestamp, s_name, s_value) VALUES (1,'IoTDB', 2)");
        adminStmt.execute("REVOKE SYSTEM ON root.** FROM USER tempuser");

        Assert.assertThrows(
            SQLException.class,
            () ->
                userStmt.execute(
                    "INSERT INTO root.a.d1(timestamp, s_name, s_value, s_value_2) VALUES (1,'IoTDB', 2, 2)"));
      }
    }
  }

  @Test
  public void testCreateRoleIdentifierName() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();
    adminStmt.execute("create role head");
    adminStmt.execute("create user head 'password123456'");
    adminStmt.execute("create role tail");
    adminStmt.execute("create user tail 'password123456'");
  }

  @Test
  public void testClusterManagementSqlOfTreeModel() throws Exception {
    ImmutableList<String> clusterManagementSQLList =
        ImmutableList.of(
            // show cluster, nodes, regions,
            "show ainodes",
            "show confignodes",
            "show datanodes",
            "show cluster",
            "show clusterid",
            "show regions",
            "show data regionid where database=root.**",

            // remove node
            "remove datanode 0",
            "remove confignode 0",

            // region operation
            "migrate region 0 from 1 to 2",
            "reconstruct region 0 on 1",
            "extend region 0 to 1",
            "remove region 0 from 1",

            // others
            "show timeslotid where database=root.test",
            "count timeslotid where database=root.test",
            "show data seriesslotid where database=root.test",
            "verify connection");

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER Jack 'temppw123456'");

      try (Connection JackConnection = EnvFactory.getEnv().getConnection("Jack", "temppw123456");
          Statement Jack = JackConnection.createStatement()) {
        testClusterManagementSqlImpl(
            clusterManagementSQLList,
            () -> adminStmt.execute("GRANT SYSTEM ON root.** TO USER Jack"),
            Jack);
      }
    }
  }

  @Test
  public void testClusterManagementSqlOfTableModel() throws Exception {
    ImmutableList<String> clusterManagementSQLList =
        ImmutableList.of(
            // show cluster, nodes, regions,
            "show ainodes",
            "show confignodes",
            "show datanodes",
            "show cluster",
            "show clusterid",
            "show regions",

            // remove node
            "remove datanode 0",
            "remove confignode 0",

            // region operation
            "migrate region 0 from 1 to 2",
            "reconstruct region 0 on 1",
            "extend region 0 to 1",
            "remove region 0 from 1");

    try (Connection adminCon = EnvFactory.getEnv().getTableConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER Jack 'temppw123456'");

      try (Connection JackConnection =
              EnvFactory.getEnv().getConnection("Jack", "temppw123456", BaseEnv.TABLE_SQL_DIALECT);
          Statement Jack = JackConnection.createStatement()) {
        // Jack has no authority to execute these SQLs
        for (String sql : clusterManagementSQLList) {
          try {
            Jack.execute(sql);
          } catch (IoTDBSQLException e) {
            if (TSStatusCode.NO_PERMISSION.getStatusCode() != e.getErrorCode()) {
              fail(
                  String.format(
                      "SQL should fail because of no permission, but the error code is %d: %s",
                      e.getErrorCode(), sql));
            }
            continue;
          }
          fail(String.format("SQL should fail because of no permission: %s", sql));
        }
      }
    }
  }

  private void testClusterManagementSqlImpl(
      List<String> clusterManagementSqlList, Callable<Boolean> giveJackAuthority, Statement Jack)
      throws Exception {
    // Jack has no authority to execute these SQLs
    for (String sql : clusterManagementSqlList) {
      try {
        Jack.execute(sql);
      } catch (IoTDBSQLException e) {
        if (TSStatusCode.NO_PERMISSION.getStatusCode() != e.getErrorCode()) {
          fail(
              String.format(
                  "SQL should fail because of no permission, but the error code is %d: %s",
                  e.getErrorCode(), sql));
        }
        continue;
      }
      fail(String.format("SQL should fail because of no permission: %s", sql));
    }

    // Give Jack authority
    giveJackAuthority.call();

    // Jack is able to execute these SQLs now
    for (String sql : clusterManagementSqlList) {
      try {
        // No exception is fine
        Jack.execute(sql);
      } catch (IoTDBSQLException e) {
        // If there is an exception, error code must not be NO_PERMISSION
        if (TSStatusCode.NO_PERMISSION.getStatusCode() == e.getErrorCode()) {
          fail(String.format("SQL should not fail with no permission: %s", sql));
        }
      }
    }
  }

  @Test
  public void noNeedPrivilegeTest() {
    createUser("tempuser", "temppw123456");
    String[] expectedHeader = new String[] {"CurrentUser"};
    String[] retArray =
        new String[] {
          "tempuser,",
        };
    resultSetEqualTest("show current_user", expectedHeader, retArray, "tempuser", "temppw123456");
    executeNonQuery("SHOW AVAILABLE URLS", "tempuser", "temppw123456");
  }

  @Ignore
  @Test
  public void testStrongPassword() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION 'enforce_strong_password'='true'");
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create user userA 'NO_LOWER_CASE_123'");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "820: Invalid password, must contain at least one lowercase letter.", e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter user root set password 'NO_LOWER_CASE_123'");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "820: Invalid password, must contain at least one lowercase letter.", e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create user userA 'no_upper_case_123'");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "820: Invalid password, must contain at least one uppercase letter.", e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter user root set password 'no_upper_case_123'");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "820: Invalid password, must contain at least one uppercase letter.", e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create user userA 'noooooo_DIGIT'");
      fail();
    } catch (SQLException e) {
      assertEquals("820: Invalid password, must contain at least one digit.", e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter user root set password 'noooooo_DIGIT'");
      fail();
    } catch (SQLException e) {
      assertEquals("820: Invalid password, must contain at least one digit.", e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create user userA 'noSpecial123'");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "820: Invalid password, must contain at least one special character.", e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter user root set password 'noSpecial123'");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "820: Invalid password, must contain at least one special character.", e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create user userA '1234567891011'");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "820: Invalid password, must contain at least one lowercase letter, one uppercase letter, one special character.",
          e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter user root set password '1234567891011'");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "820: Invalid password, must contain at least one lowercase letter, one uppercase letter, one special character.",
          e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create user userA123456789 'userA123456789'");
      fail();
    } catch (SQLException e) {
      assertEquals("820: Password cannot be the same as user name", e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter user root set password 'root'");
      fail();
    } catch (SQLException e) {
      assertEquals("820: Password cannot be the same as user name", e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create user userA 'strongPassword#1234'");

      statement.execute("SET CONFIGURATION 'enforce_strong_password'='false'");

      statement.execute("create user userB '123412345678'");
      statement.execute("alter user root set password 'root12345678'");
    }
  }

  @Test
  public void testPasswordHistory() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      testPasswordHistoryEncrypted(statement);
      testPasswordHistoryCreateAndDrop(statement);
      testPasswordHistoryAlter(statement);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public void testPasswordHistoryEncrypted(Statement statement) throws SQLException {
    ResultSet resultSet =
        statement.executeQuery("SELECT password,oldPassword from root.__audit.password_history._0");
    assertTrue(resultSet.next());
    assertEquals(
        AuthUtils.encryptPassword(CommonDescriptor.getInstance().getConfig().getAdminPassword()),
        resultSet.getString("root.__audit.password_history._0.password"));
    assertEquals(
        AuthUtils.encryptPassword(CommonDescriptor.getInstance().getConfig().getAdminPassword()),
        resultSet.getString("root.__audit.password_history._0.oldPassword"));
  }

  public void testPasswordHistoryCreateAndDrop(Statement statement) throws SQLException {
    statement.execute("create user userA 'abcdef123456'");

    long expectedUserAId = INTERNAL_USER_END_ID + 1;
    try (ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "select last password from %s.`_" + expectedUserAId + "`",
                PREFIX_PASSWORD_HISTORY))) {
      if (!resultSet.next()) {
        fail("Password history not found");
      }
      assertEquals(AuthUtils.encryptPassword("abcdef123456"), resultSet.getString("Value"));
    }

    try (ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "select last oldPassword from %s.`_" + expectedUserAId + "`",
                PREFIX_PASSWORD_HISTORY))) {
      if (!resultSet.next()) {
        fail("Password history not found");
      }
      assertEquals(AuthUtils.encryptPassword("abcdef123456"), resultSet.getString("Value"));
    }

    statement.execute("drop user userA");

    try (ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "select last password from %s.`_" + expectedUserAId + "`",
                PREFIX_PASSWORD_HISTORY))) {
      assertFalse(resultSet.next());
    }

    try (ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "select last oldPassword from %s.`_" + expectedUserAId + "`",
                PREFIX_PASSWORD_HISTORY))) {
      assertFalse(resultSet.next());
    }
  }

  public void testPasswordHistoryAlter(Statement statement) throws SQLException {
    statement.execute("create user userA 'abcdef123456'");
    statement.execute("alter user userA set password 'abcdef654321'");

    long expectedUserAId = INTERNAL_USER_END_ID + 2;
    try (ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "select last password from %s.`_" + expectedUserAId + "`",
                PREFIX_PASSWORD_HISTORY))) {
      if (!resultSet.next()) {
        fail("Password history not found");
      }
      assertEquals(AuthUtils.encryptPassword("abcdef654321"), resultSet.getString("Value"));
    }

    try (ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "select oldPassword from %s.`_" + expectedUserAId + "` order by time desc limit 1",
                PREFIX_PASSWORD_HISTORY))) {
      if (!resultSet.next()) {
        fail("Password history not found");
      }
      assertEquals(
          AuthUtils.encryptPassword("abcdef123456"),
          resultSet.getString(
              String.format("%s._" + expectedUserAId + ".oldPassword", PREFIX_PASSWORD_HISTORY)));
    }
  }

  @Test
  public void testChangeBackPassword() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER USER root SET PASSWORD 'newPassword666888'");
      statement.execute("ALTER USER root SET PASSWORD 'rootHasANewPassword123'");
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSecurityPrivilege() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER security_user 'abcdef123456'");
      statement.execute("CREATE USER common_user 'abcdef123456'");
      try (Connection userCon = EnvFactory.getEnv().getConnection("security_user", "abcdef123456");
          Statement userStatement = userCon.createStatement()) {
        Assert.assertThrows(
            SQLException.class,
            () -> userStatement.execute("GRANT SYSTEM ON root.** TO USER common_user"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStatement.execute("REVOKE SYSTEM ON root.** FROM USER common_user"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStatement.executeQuery("LIST PRIVILEGES OF USER common_user"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStatement.execute("CREATE USER common_user2 'abcdef123456'"));
        Assert.assertThrows(
            SQLException.class, () -> userStatement.execute("CREATE ROLE common_role"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStatement.execute("GRANT SYSTEM ON root.** TO ROLE common_role"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStatement.execute("REVOKE SYSTEM ON root.** FROM ROLE common_role"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStatement.execute("GRANT ROLE common_role TO common_user"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStatement.execute("REVOKE ROLE common_role FROM common_user"));
        Assert.assertThrows(
            SQLException.class, () -> userStatement.execute("DROP USER common_user2"));
        Assert.assertThrows(
            SQLException.class, () -> userStatement.execute("DROP ROLE common_role"));
      }
      statement.execute("GRANT SECURITY ON root.** TO USER security_user");
      try (Connection userCon = EnvFactory.getEnv().getConnection("security_user", "abcdef123456");
          Statement userStatement = userCon.createStatement()) {
        userStatement.execute("GRANT SYSTEM ON root.** TO USER common_user");
        userStatement.execute("REVOKE SYSTEM ON root.** FROM USER common_user");
        ResultSet resultSet = userStatement.executeQuery("LIST PRIVILEGES OF USER common_user");
        Assert.assertFalse(resultSet.next());
        userStatement.execute("CREATE USER common_user2 'abcdef123456'");
        userStatement.execute("CREATE ROLE common_role");
        userStatement.execute("GRANT SYSTEM ON root.** TO ROLE common_role");
        userStatement.execute("REVOKE SYSTEM ON root.** FROM ROLE common_role");
        userStatement.execute("GRANT ROLE common_role TO common_user");
        userStatement.execute("REVOKE ROLE common_role FROM common_user");
        userStatement.execute("DROP USER common_user2");
        userStatement.execute("DROP ROLE common_role");
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAudit() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("grant read_data on root.__audit to user user2");
      } catch (SQLException e) {
        assertEquals(
            "803: Access Denied: Cannot grant or revoke any privileges to root.__audit",
            e.getMessage());
      }
      try {
        statement.execute("revoke read_data on root.__audit from user user2");
      } catch (SQLException e) {
        assertEquals(
            "803: Access Denied: Cannot grant or revoke any privileges to root.__audit",
            e.getMessage());
      }
      try {
        statement.execute("grant read_data on root.__audit to role role1");
      } catch (SQLException e) {
        assertEquals(
            "803: Access Denied: Cannot grant or revoke any privileges to root.__audit",
            e.getMessage());
      }
      try {
        statement.execute("revoke read_data on root.__audit from role role1");
      } catch (SQLException e) {
        assertEquals(
            "803: Access Denied: Cannot grant or revoke any privileges to root.__audit",
            e.getMessage());
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
