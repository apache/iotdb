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

package org.apache.iotdb.postgresql.it;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Password login on loopback, refusals (28P01), the connection limit and read-only statements.
 * Failed PostgreSQL logins feed the database's login lock like any other login.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBPostgreSqlSecurityIT {

  private static int port;

  @BeforeClass
  public static void setUp() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setEnablePostgreSqlService(true)
        .setPostgreSqlServicePort(port)
        .setPostgreSqlServiceMaxConnections(2);
    EnvFactory.getEnv().initClusterEnvironment();
    PgItSupport.awaitListening(port);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static Connection connect(String user, String password) throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("sslmode", "disable");
    return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + port + "/any", props);
  }

  private static SQLException refused(String user, String password) {
    try (Connection ignored = connect(user, password)) {
      fail("refused: " + user);
      return null;
    } catch (SQLException e) {
      return e;
    }
  }

  @Test
  public void loginQueryAndRefusals() throws Exception {
    try (Connection c = connect(SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
        Statement s = c.createStatement()) {
      try (ResultSet rs = s.executeQuery("SELECT current_user")) {
        assertTrue(rs.next());
        assertEquals(SessionConfig.DEFAULT_USER, rs.getString(1));
      }
      try {
        s.execute("CREATE DATABASE x");
        fail("read-only");
      } catch (SQLException e) {
        assertEquals("25006", e.getSQLState());
      }
    }
    SQLException wrong = refused(SessionConfig.DEFAULT_USER, "Wrong-Pass-2026");
    SQLException unknown = refused("nobody", "Wrong-Pass-2026");
    assertEquals("28P01", wrong.getSQLState());
    assertEquals("28P01", unknown.getSQLState());
    // the messages are the database's own login messages
    assertTrue(wrong.getMessage(), wrong.getMessage().contains("Authentication failed."));
    assertTrue(unknown.getMessage(), unknown.getMessage().contains("nobody"));
  }

  @Test
  public void theConnectionLimitHolds() throws Exception {
    try (Connection first = connect(SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
        Connection second = connect(SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD)) {
      SQLException third = refused(SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
      assertEquals(third.getMessage(), "53300", third.getSQLState());
      assertTrue(first.isValid(5) && second.isValid(5));
    }
  }
}
