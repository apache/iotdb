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
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** This is an example for integration test. */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAuthIT {

  @Before
  public void setUp() throws Exception {
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
      adminStmt.execute("CREATE USER tempuser 'temppw'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
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

        //  2. admin grant all privileges to user tempuser, So tempuser can do anything.
        adminStmt.execute("GRANT ALL ON root.** TO USER tempuser");

        userStmt.execute("CREATE DATABASE root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (100, 100)");
        userStmt.execute("SELECT * from root.a");

        // 3. All privileges granted to tempuser cannot be delegated
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT WRITE_SCHEMA ON root.a TO USER tempuser"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT WRITE_SCHEMA ON root.b.b TO USER tempuser"));

        // 4. Admin grant write_schema, read_schema on root.** to tempuser.
        adminStmt.execute("GRANT WRITE_SCHEMA ON root.** TO USER tempuser WITH GRANT OPTION");
        adminStmt.execute("GRANT READ_SCHEMA ON root.** TO USER tempuser WITH GRANT OPTION");

        // 5. tempuser can grant or revoke his write_schema and read_schema to others or himself.
        userStmt.execute("GRANT WRITE_SCHEMA ON root.t1 TO USER tempuser");
        userStmt.execute("GRANT READ_SCHEMA ON root.t1.t2 TO USER tempuser WITH GRANT OPTION");
        userStmt.execute("REVOKE WRITE_SCHEMA ON root.t1 FROM USER tempuser");
        // tempuser revoke his write_schema privilege
        userStmt.execute("REVOKE WRITE_SCHEMA ON root.** FROM USER tempuser");

        // 6. REVOKE ALL will get an error.
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("REVOKE ALL on root.** FROM USER tempuser"));
        adminStmt.execute("GRANT ALL ON root.** TO USER tempuser");
        adminStmt.execute("REVOKE ALL ON root.** FROM USER tempuser");

        Assert.assertThrows(SQLException.class, () -> userStmt.execute("CREATE DATABASE root.b"));

        // 7. With "WRITE" privilege, tempuser can read,write schema or data.
        adminStmt.execute("GRANT WRITE, MANAGE_DATABASE on root.** TO USER tempuser");
        userStmt.execute("CREATE DATABASE root.c");
        userStmt.execute("CREATE TIMESERIES root.c.d WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.c(timestamp, d) VALUES (100, 100)");
        ResultSet result = userStmt.executeQuery("SELECT * from root.c");
        String ans = "100,100,\n";
        validateResultSet(result, ans);

        adminStmt.execute("REVOKE WRITE, MANAGE_DATABASE on root.** FROM USER tempuser");
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
      }
    }
  }

  @Test
  public void testSetDeleteSG() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER sgtest 'sgtest'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("sgtest", "sgtest");
          Statement userStmt = userCon.createStatement()) {

        Assert.assertThrows(
            SQLException.class, () -> userStmt.execute("CREATE DATABASE root.sgtest"));

        adminStmt.execute("GRANT MANAGE_DATABASE ON root.** TO USER sgtest");

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
          SQLException.class, () -> adminStmt.execute("CREATE USER tempuser 'temppw '"));
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
      adminStmt.execute("CREATE USER tempuser 'temppw'");
      Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
      userCon.close();

      adminStmt.execute("ALTER USER tempuser SET PASSWORD 'newpw'");

      boolean caught = false;
      try {
        userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
      } catch (SQLException e) {
        caught = true;
      } finally {
        userCon.close();
      }
      assertTrue(caught);

      userCon = EnvFactory.getEnv().getConnection("tempuser", "newpw");

      userCon.close();
    }
  }

  @Test
  public void illegalGrantRevokeUserTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        // grant a non-existing user
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("GRANT WRITE_SCHEMA on root.a TO USER 'not a user'"));
        // grant a non-existing privilege
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("GRANT NOT_A_PRIVILEGE on root.a TO USER tempuser"));
        adminStmt.execute("GRANT MANAGE_USER on root.** TO USER tempuser");
        // grant on an illegal seriesPath
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("GRANT WRITE_SCHEMA on a.b TO USER tempuser"));
        // grant admin
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("GRANT WRITE_SCHEMA on root.a.b TO USER root"));
        // no privilege to grant
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT WRITE_SCHEMA on root.a.b TO USER tempuser"));
        // revoke a non-existing privilege
        adminStmt.execute("REVOKE MANAGE_USER on root.** FROM USER tempuser");
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("REVOKE MANAGE_USER on root.** FROM USER tempuser"));
        // revoke a non-existing user
        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("REVOKE MANAGE_USER on root.** FROM USER tempuser1"));
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

      adminStmt.execute("CREATE USER tempuser 'temppw'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        // grant and revoke the user the privilege to create time series
        Assert.assertThrows(SQLException.class, () -> userStmt.execute("CREATE DATABASE root.a"));

        adminStmt.execute("GRANT MANAGE_DATABASE,WRITE_SCHEMA ON root.** TO USER tempuser");
        userStmt.execute("CREATE DATABASE root.a");
        adminStmt.execute("GRANT WRITE_SCHEMA ON root.a.b TO USER tempuser");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("CREATE DATABASE root.b");
        // grant again wil success.
        adminStmt.execute("GRANT MANAGE_DATABASE,WRITE_SCHEMA ON root.** TO USER tempuser");

        adminStmt.execute("REVOKE MANAGE_DATABASE,WRITE_SCHEMA ON root.** FROM USER tempuser");
        // no privilege to create this one anymore
        Assert.assertThrows(
            SQLException.class,
            () ->
                userStmt.execute("CREATE TIMESERIES root.b.a WITH DATATYPE=INT32,ENCODING=PLAIN"));

        Assert.assertThrows(
            SQLException.class,
            () -> adminStmt.execute("REVOKE WRITE_SCHEMA ON root.a.b FROM USER tempuser"));
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
      adminStmt.execute("CREATE USER tempuser 'temppw'");
      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
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
      adminStmt.execute("CREATE USER tempuser 'temppw'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        adminStmt.execute("GRANT MANAGE_DATABASE ON root.** TO USER tempuser");
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

      adminStmt.execute("CREATE USER tempuser 'temppw'");
      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        Assert.assertThrows(SQLException.class, () -> userStmt.execute("CREATE ROLE admin"));

        adminStmt.execute("CREATE ROLE admin");
        adminStmt.execute("GRANT MANAGE_DATABASE,WRITE_SCHEMA,WRITE_DATA on root.** TO ROLE admin");
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
        ResultSet resultSet = userStmt.executeQuery("SELECT * FROM root.**");
        validateResultSet(resultSet, "1,100,1000,\n");
        resultSet.close();

        adminStmt.execute("REVOKE MANAGE_DATABASE,WRITE_SCHEMA on root.** FROM ROLE admin");
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
      adminStmt.execute("CREATE USER user1 'password'");
      adminStmt.execute("CREATE USER user2 'password'");
      adminStmt.execute("CREATE ROLE role1");
      adminStmt.execute("CREATE ROLE role2");
      adminStmt.execute("GRANT ROLE role1 TO user1");
      adminStmt.execute("GRANT ROLE role1 TO user2");
      adminStmt.execute("GRANT ROLE role2 TO user2");
      adminStmt.execute("GRANT MANAGE_ROLE,MANAGE_USER ON root.** TO USER user1");

      // user1 : role1; MANAGE_ROLE,MANAGE_USER
      // user2 : role1, role2;
      ResultSet resultSet;
      String ans = "";
      Connection userCon = EnvFactory.getEnv().getConnection("user1", "password");
      Statement userStmt = userCon.createStatement();
      try {
        resultSet = userStmt.executeQuery("LIST ROLE OF USER user1");
        ans = "role1,\n";
        validateResultSet(resultSet, ans);
        resultSet = userStmt.executeQuery("LIST ROLE OF USER user2");
        ans = "role1,\nrole2,\n";
        validateResultSet(resultSet, ans);
        resultSet = userStmt.executeQuery("LIST USER OF ROLE role1");
        ans = "user1,\nuser2,\n";
        validateResultSet(resultSet, ans);
      } finally {
        userStmt.close();
        userCon.close();
      }

      Connection user2Con = EnvFactory.getEnv().getConnection("user2", "password");
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
      adminStmt.execute("CREATE USER user1 'password1'");
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
            ",root.**,MANAGE_USER,true,\n"
                + ",root.**,MANAGE_ROLE,true,\n"
                + ",root.**,USE_TRIGGER,true,\n"
                + ",root.**,USE_UDF,true,\n"
                + ",root.**,USE_CQ,true,\n"
                + ",root.**,USE_PIPE,true,\n"
                + ",root.**,USE_MODEL,true,\n"
                + ",root.**,EXTEND_TEMPLATE,true,\n"
                + ",root.**,MANAGE_DATABASE,true,\n"
                + ",root.**,MAINTAIN,true,\n"
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
      adminStmt.execute("CREATE USER chenduxiu 'orange'");

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
        adminStmt.execute("CREATE USER " + members[i] + " 'a666666'");
        adminStmt.execute("GRANT ROLE dalao TO  " + members[i]);
      }
      adminStmt.execute("CREATE USER RiverSky 'a2333333'");
      adminStmt.execute("GRANT ROLE zhazha TO RiverSky");

      ResultSet resultSet = adminStmt.executeQuery("LIST USER OF ROLE dalao");
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

        resultSet = adminStmt.executeQuery("LIST USER OF ROLE zhazha");
        ans = "RiverSky,\n";
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
      adminStmt.execute("CREATE USER user" + i + " 'password" + i + "'");
    }

    adminStmt.execute("CREATE USER tempuser 'temppw'");

    try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
        Statement userStmt = userCon.createStatement()) {
      try {
        Assert.assertThrows(SQLException.class, () -> userStmt.execute("LIST USER"));
        // with list user privilege
        adminStmt.execute("GRANT MANAGE_USER on root.** TO USER tempuser");
        ResultSet resultSet = userStmt.executeQuery("LIST USER");
        String ans =
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
  public void testExecuteBatchWithPrivilege() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection("root", "root");
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw'");
      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
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
      adminStmt.execute("CREATE USER tempuser 'temppw'");
      adminStmt.execute("GRANT WRITE_DATA on root.sg1.** TO USER tempuser");
      adminStmt.execute("GRANT WRITE_SCHEMA on root.sg1.** TO USER tempuser");
      adminStmt.execute("GRANT MANAGE_DATABASE on root.** TO USER tempuser");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
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
      adminStatement.execute("CREATE USER a_application 'a_application'");
      adminStatement.execute("CREATE ROLE application_role");
      adminStatement.execute("GRANT READ_DATA ON root.test.** TO ROLE application_role");
      adminStatement.execute("GRANT ROLE application_role TO a_application");

      adminStatement.execute("INSERT INTO root.test(time, s1, s2, s3) VALUES(1, 2, 3, 4)");
    }

    try (Connection userConnection =
            EnvFactory.getEnv().getConnection("a_application", "a_application");
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
      adminStatement.execute("CREATE USER user01 'pass1234'");
      adminStatement.execute("CREATE USER user02 'pass1234'");
      adminStatement.execute("CREATE ROLE manager");
      adminStatement.execute("GRANT MANAGE_ROLE on root.** TO USER user01");
      Assert.assertThrows(
          SQLException.class, () -> adminStatement.execute("GRANT role manager to `root`"));
    }

    try (Connection userCon = EnvFactory.getEnv().getConnection("user01", "pass1234");
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
    adminStmt.execute("CREATE USER user1 'password'");
    adminStmt.execute("CREATE USER user2 'password'");
    adminStmt.execute("CREATE USER user3 'password'");
    adminStmt.execute("CREATE ROLE testRole");
    adminStmt.execute("GRANT manage_database ON root.** TO ROLE testRole WITH GRANT OPTION");
    adminStmt.execute("GRANT READ_DATA ON root.t1.** TO ROLE testRole");
    adminStmt.execute("GRANT READ_SCHEMA ON root.t3.t2.** TO ROLE testRole WITH GRANT OPTION");

    // 2. USER1 has all privileges on root.**
    for (PrivilegeType item : PrivilegeType.values()) {
      String sql = "GRANT %s on root.** to USER user1";
      adminStmt.execute(String.format(sql, item.toString()));
    }
    // 3.admin lists privileges of user1
    ResultSet resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF USER user1");
    String ans =
        ",root.**,MANAGE_USER,false,\n"
            + ",root.**,MANAGE_ROLE,false,\n"
            + ",root.**,USE_TRIGGER,false,\n"
            + ",root.**,USE_UDF,false,\n"
            + ",root.**,USE_CQ,false,\n"
            + ",root.**,USE_PIPE,false,\n"
            + ",root.**,USE_MODEL,false,\n"
            + ",root.**,EXTEND_TEMPLATE,false,\n"
            + ",root.**,MANAGE_DATABASE,false,\n"
            + ",root.**,MAINTAIN,false,\n"
            + ",root.**,READ_DATA,false,\n"
            + ",root.**,WRITE_DATA,false,\n"
            + ",root.**,READ_SCHEMA,false,\n"
            + ",root.**,WRITE_SCHEMA,false,\n";
    validateResultSet(resultSet, ans);

    // 4. USER2 has all privilegs on root.** with grant option;
    for (PrivilegeType item : PrivilegeType.values()) {
      String sql = "GRANT %s on root.** to USER user2 with grant option";
      adminStmt.execute(String.format(sql, item.toString()));
    }
    resultSet = adminStmt.executeQuery("LIST PRIVILEGES OF USER user2");
    ans =
        ",root.**,MANAGE_USER,true,\n"
            + ",root.**,MANAGE_ROLE,true,\n"
            + ",root.**,USE_TRIGGER,true,\n"
            + ",root.**,USE_UDF,true,\n"
            + ",root.**,USE_CQ,true,\n"
            + ",root.**,USE_PIPE,true,\n"
            + ",root.**,USE_MODEL,true,\n"
            + ",root.**,EXTEND_TEMPLATE,true,\n"
            + ",root.**,MANAGE_DATABASE,true,\n"
            + ",root.**,MAINTAIN,true,\n"
            + ",root.**,READ_DATA,true,\n"
            + ",root.**,WRITE_DATA,true,\n"
            + ",root.**,READ_SCHEMA,true,\n"
            + ",root.**,WRITE_SCHEMA,true,\n";
    validateResultSet(resultSet, ans);

    // now user1 has all privileges, user2 has all privileges with grant option, user3 doesn't have
    // privileges

    // 5. Login user1 to list user2 privileges will success
    // user1 cannot grant any privilegs to user3
    try (Connection userCon = EnvFactory.getEnv().getConnection("user1", "password");
        Statement userStmt = userCon.createStatement()) {
      try {
        resultSet = userStmt.executeQuery("LIST PRIVILEGES OF USER user1");
        ans =
            ",root.**,MANAGE_USER,false,\n"
                + ",root.**,MANAGE_ROLE,false,\n"
                + ",root.**,USE_TRIGGER,false,\n"
                + ",root.**,USE_UDF,false,\n"
                + ",root.**,USE_CQ,false,\n"
                + ",root.**,USE_PIPE,false,\n"
                + ",root.**,USE_MODEL,false,\n"
                + ",root.**,EXTEND_TEMPLATE,false,\n"
                + ",root.**,MANAGE_DATABASE,false,\n"
                + ",root.**,MAINTAIN,false,\n"
                + ",root.**,READ_DATA,false,\n"
                + ",root.**,WRITE_DATA,false,\n"
                + ",root.**,READ_SCHEMA,false,\n"
                + ",root.**,WRITE_SCHEMA,false,\n";
        validateResultSet(resultSet, ans);
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT MANAGE_ROLE ON root.** TO USER user3"));
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("REVOKE MANAGE_ROLE ON root.** FROM USER user2"));
      } finally {
        userStmt.close();
      }
    }
    // 6.Login user2 grant and revoke will success.
    try (Connection userCon = EnvFactory.getEnv().getConnection("user2", "password");
        Statement userStmt = userCon.createStatement()) {
      try {
        resultSet = userStmt.executeQuery("LIST PRIVILEGES OF USER user1");
        validateResultSet(resultSet, ans);
        userStmt.execute("GRANT MANAGE_ROLE ON root.** TO USER user3");
        resultSet = userStmt.executeQuery("LIST PRIVILEGES OF USER user3");
        ans = ",root.**,MANAGE_ROLE,false,\n";
        validateResultSet(resultSet, ans);

        userStmt.execute("REVOKE MANAGE_ROLE ON root.** FROM USER user1");
        resultSet = userStmt.executeQuery("LIST PRIVILEGES OF USER user1");
        ans =
            ",root.**,MANAGE_USER,false,\n"
                + ",root.**,USE_TRIGGER,false,\n"
                + ",root.**,USE_UDF,false,\n"
                + ",root.**,USE_CQ,false,\n"
                + ",root.**,USE_PIPE,false,\n"
                + ",root.**,USE_MODEL,false,\n"
                + ",root.**,EXTEND_TEMPLATE,false,\n"
                + ",root.**,MANAGE_DATABASE,false,\n"
                + ",root.**,MAINTAIN,false,\n"
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
    // now user has:
    // 1. MANAGE_ROLE
    // 2. MANAGE_DATABASE with grant option
    // 3. READ_DATA on root.t1.**
    // 4. READ_SCHEMA on root.t3.t2.**

    try (Connection userCon = EnvFactory.getEnv().getConnection("user3", "password");
        Statement userStmt = userCon.createStatement()) {
      try {
        // because role's privilege
        userStmt.execute("GRANT manage_database ON root.** TO USER user1");
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

    adminStmt.close();
  }

  @Test
  public void testRevokeAndGrantOpt() throws SQLException {
    // 1. revoke from user/role
    Connection adminCon = EnvFactory.getEnv().getConnection();
    Statement adminStmt = adminCon.createStatement();
    adminStmt.execute("CREATE USER user1 'password'");
    adminStmt.execute("CREATE USER user2 'password'");

    // 2. user1 has all privileges with grant option on root.**
    //    user2 has all privileges without grant option on root.**
    //    user2 has all privileges without grant option on root.t1.**
    for (PrivilegeType item : PrivilegeType.values()) {
      String sql = "GRANT %s on root.** to USER user1 WITH GRANT OPTION";
      adminStmt.execute(String.format(sql, item));
      if (item.isPathRelevant()) {
        adminStmt.execute(String.format("GRANT %s on root.t1.** TO USER user2", item));
      }
      sql = "GRANT %s on root.** to USER user2";
      adminStmt.execute(String.format(sql, item));
    }
    Connection user1Con = EnvFactory.getEnv().getConnection("user1", "password");
    Statement user1Stmt = user1Con.createStatement();
    ResultSet resultSet;
    String ans = "";
    try {
      // revoke privileges on root.** and root.t1.**
      for (PrivilegeType item : PrivilegeType.values()) {
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
    adminStmt.execute("CREATE USER user1 'password'");
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
    try (ResultSet resultSet = adminStmt.executeQuery("select * from root.**;")) {
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
    Connection user1Con = EnvFactory.getEnv().getConnection("user1", "password");
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
      adminStmt.execute("CREATE USER tempuser 'temppw'");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {

        adminStmt.execute("CREATE DATABASE root.a");
        adminStmt.execute("create schema template t1 aligned (s_name TEXT)");
        adminStmt.execute("GRANT EXTEND_TEMPLATE ON root.** TO USER tempuser");
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
        adminStmt.execute("REVOKE EXTEND_TEMPLATE ON root.** FROM USER tempuser");

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
    adminStmt.execute("create user head 'password'");
    adminStmt.execute("create role tail");
    adminStmt.execute("create user tail 'password'");
  }
}
