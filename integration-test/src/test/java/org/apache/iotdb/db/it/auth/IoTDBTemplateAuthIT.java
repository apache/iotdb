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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.CHILD_NODES;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PATHS;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.TEMPLATE_NAME;
import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTemplateAuthIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void manageDataBaseTest() {

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create device template t1 (temperature1 FLOAT, status1 BOOLEAN)");
      adminStmt.execute("create device template t2 (temperature2 FLOAT, status2 BOOLEAN)");
      adminStmt.execute("create device template t3 (temperature3 FLOAT, status3 BOOLEAN)");
      adminStmt.execute("create user tytyty1 'tytytytytytytyty'");
      adminStmt.execute("create user tytyty2 'tytytytytytytyty'");
      adminStmt.execute("create user tytyty3 'tytytytytytytyty'");

      assertNonQueryTestFail(
          "create device template t4 (temperature4 FLOAT, status4 BOOLEAN)",
          "803: No permissions for this operation, please add privilege SYSTEM",
          "tytyty1",
          "tytytytytytytyty");

      assertNonQueryTestFail(
          adminStmt,
          "create database root.__audit",
          "803: The database 'root.__audit' is read-only");

      assertNonQueryTestFail(
          adminStmt,
          "set device template t1 to root.__audit",
          "803: The database 'root.__audit' is read-only");

      Set<String> retSet = new HashSet<>(Arrays.asList("t1", "t2", "t3"));

      try (ResultSet resultSet = adminStmt.executeQuery("show device templates")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TEMPLATE_NAME);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show device templates")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }

      adminStmt.execute("create database root.sg1");
      adminStmt.execute("create database root.sg2");
      adminStmt.execute("create database root.sg3");
      adminStmt.execute("set device template t1 to root.sg1.d1");

      adminStmt.execute("set device template t2 to root.sg2");

      assertNonQueryTestFail(
          "set device template t1 to root.sg3",
          "803: No permissions for this operation, please add privilege SYSTEM",
          "tytyty1",
          "tytytytytytytyty");

      adminStmt.execute("set device template t1 to root.sg3");
      adminStmt.execute("create timeseries using device template on root.sg1.d1");
      adminStmt.execute("create timeseries using device template on root.sg3");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show device templates")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }

      adminStmt.execute("grant read_schema on root.sg1.d2.** to user tytyty1");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show device templates")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }

      adminStmt.execute("grant read_schema on root.sg1.d1.** to user tytyty1");
      retSet = Collections.singleton("t1");
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show device templates")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TEMPLATE_NAME);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      adminStmt.execute("grant read_schema on root.sg1.** to user tytyty2");
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty2", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show device templates")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TEMPLATE_NAME);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      adminStmt.execute("grant read_schema on root.** to user tytyty3");
      retSet = new HashSet<>(Arrays.asList("t1", "t2"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty3", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show device templates")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TEMPLATE_NAME);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      // show nodes in device template
      retSet = new HashSet<>(Arrays.asList("temperature1", "status1"));
      try (ResultSet resultSet = adminStmt.executeQuery("show nodes in device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty2", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty3", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      retSet = new HashSet<>(Arrays.asList("temperature2", "status2"));
      try (ResultSet resultSet = adminStmt.executeQuery("show nodes in device template t2")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t2")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty2", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t2")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty3", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t2")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      retSet = new HashSet<>(Arrays.asList("temperature3", "status3"));
      try (ResultSet resultSet = adminStmt.executeQuery("show nodes in device template t3")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t3")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty2", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t3")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty3", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t3")) {
        while (resultSet.next()) {
          fail("should see nothing");
        }
      }

      // show paths set device template
      retSet = new HashSet<>(Arrays.asList("root.sg1.d1", "root.sg3"));
      try (ResultSet resultSet = adminStmt.executeQuery("show paths set device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty3", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      retSet = new HashSet<>(Arrays.asList("root.sg1.d1"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty2", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      retSet = new HashSet<>(Arrays.asList("root.sg2"));
      try (ResultSet resultSet = adminStmt.executeQuery("show paths set device template t2")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t2")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty2", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t2")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty3", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t2")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      try (ResultSet resultSet = adminStmt.executeQuery("show paths set device template t3")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t3")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty2", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t3")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty3", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t3")) {
        while (resultSet.next()) {
          fail("should see nothing");
        }
      }

      // show paths using device template
      retSet = new HashSet<>(Arrays.asList("root.sg1.d1", "root.sg3"));
      try (ResultSet resultSet = adminStmt.executeQuery("show paths using device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty3", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths using device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      retSet = new HashSet<>(Arrays.asList("root.sg1.d1"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths using device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty2", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths using device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      // alter template
      assertNonQueryTestFail(
          "alter device template t1 add (speed FLOAT)",
          "803: No permissions for this operation, please add privilege SYSTEM",
          "tytyty1",
          "tytytytytytytyty");

      adminStmt.execute("grant SYSTEM on root.** to user tytyty1");
      executeNonQuery("alter device template t1 add (speed FLOAT)", "tytyty1", "tytytytytytytyty");

      retSet = new HashSet<>(Arrays.asList("t1", "t2", "t3"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show device templates")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TEMPLATE_NAME);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      retSet = new HashSet<>(Arrays.asList("temperature1", "status1", "speed"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      retSet = new HashSet<>(Arrays.asList("temperature2", "status2"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t2")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      retSet = new HashSet<>(Arrays.asList("temperature3", "status3"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show nodes in device template t3")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(CHILD_NODES);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      retSet = new HashSet<>(Arrays.asList("root.sg1.d1", "root.sg3"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }
      retSet = new HashSet<>(Arrays.asList("root.sg2"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t2")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths set device template t3")) {
        while (resultSet.next()) {
          fail("should see nothing.");
        }
      }

      retSet = new HashSet<>(Arrays.asList("root.sg1.d1"));
      try (Connection userCon = EnvFactory.getEnv().getConnection("tytyty1", "tytytytytytytyty");
          Statement userStmt = userCon.createStatement();
          ResultSet resultSet = userStmt.executeQuery("show paths using device template t1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(PATHS);
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }
}
