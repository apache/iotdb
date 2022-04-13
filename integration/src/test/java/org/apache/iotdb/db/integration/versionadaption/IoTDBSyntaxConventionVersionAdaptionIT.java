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
package org.apache.iotdb.db.integration.versionadaption;

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;
import org.apache.iotdb.jdbc.Constant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBSyntaxConventionVersionAdaptionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  public void testExpression1() {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.1 with datatype = INT32");
      boolean hasResult = statement.execute("SELECT 1 FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      ResultSet resultSet = statement.getResultSet();
      Assert.assertFalse(resultSet.next());
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testExpression2() {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.1 with datatype = INT64");
      boolean hasResult = statement.execute("SELECT sin(1) FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      ResultSet resultSet = statement.getResultSet();
      Assert.assertFalse(resultSet.next());
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testExpression3() {
    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.\"a\" with datatype = INT64");
      boolean hasResult = statement.execute("SELECT \"a\" FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      ResultSet resultSet = statement.getResultSet();
      Assert.assertFalse(resultSet.next());
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }
}
