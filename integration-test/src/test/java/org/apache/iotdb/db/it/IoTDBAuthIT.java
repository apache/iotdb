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

package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** This is an example for integration test. */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAuthIT {
  private static Statement statement;
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    connection = EnvFactory.getEnv().getConnection();
    statement = connection.createStatement();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testGrantRevokeUser() {

    try {
      statement.execute("CREATE USER tempuser 'temppw'");
    } catch (SQLException e) {
      fail();
    }

    // grant create user
    try {
      statement.execute("GRANT USER tempuser PRIVILEGES CREATE_USER");
    } catch (SQLException ignored) {
      fail();
    }

    // revoke create user
    try {
      statement.execute("REVOKE USER tempuser PRIVILEGES CREATE_USER");
    } catch (SQLException ignored) {
      fail();
    }

    // duplicate grant create user
    try {
      statement.execute("GRANT USER tempuser PRIVILEGES CREATE_USER");
    } catch (SQLException e1) {
      fail();
    }
    boolean caught = false;
    try {
      statement.execute("GRANT USER tempuser PRIVILEGES CREATE_USER");
    } catch (SQLException e) {
      caught = true;
    }
    assertTrue(caught);

    // duplicate revoke create user
    try {
      statement.execute("REVOKE USER tempuser PRIVILEGES CREATE_USER");
    } catch (SQLException e1) {
      fail();
    }
    caught = false;
    try {
      statement.execute("REVOKE USER tempuser PRIVILEGES CREATE_USER");
    } catch (SQLException e) {
      caught = true;
    }
    assertTrue(caught);
  }
}
