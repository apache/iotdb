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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Ignore
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGrantOptionIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableGrantOption(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void grantTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser 'temppw'");
      adminStmt.execute("CREATE USER tempuser2 'temppw2'");
      // with grant option is disabled.
      Assert.assertThrows(
          SQLException.class,
          () -> adminStmt.execute("GRANT ALL ON root.** TO USER tempuser WITH GRANT OPTION"));
      adminStmt.execute("GRANT ALL ON root.** TO USER tempuser");
      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser", "temppw");
          Statement userStmt = userCon.createStatement()) {
        userStmt.execute("CREATE DATABASE root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (100, 100)");
        userStmt.execute("SELECT * from root.a");
        // tempuser can not grant privileges to other users
        Assert.assertThrows(
            SQLException.class, () -> userStmt.execute("GRANT ALL ON root.** TO USER tempuser2"));
        // with grant option is disabled
        Assert.assertThrows(
            SQLException.class,
            () -> userStmt.execute("GRANT ALL ON root.** TO USER tempuser2 WITH GRANT OPTION"));
      }
    }
  }
}
