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
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBFlushQueryMergeIT {

  private static String[] sqls =
      new String[] {
        "CREATE DATABASE root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
        "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
        "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
        "flush",
        "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }

  @Test
  public void selectAllSQLTest() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**"); ) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
      }
      statement.execute("merge");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFlushGivenGroup() {
    String insertTemplate =
        "INSERT INTO root.group%d(timestamp, s1, s2, s3) VALUES (%d, %d, %f, %s)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.group1");
      statement.execute("CREATE DATABASE root.group2");
      statement.execute("CREATE DATABASE root.group3");

      for (int i = 1; i <= 3; i++) {
        for (int j = 10; j < 20; j++) {
          statement.execute(String.format(insertTemplate, i, j, j, j * 0.1, j));
        }
      }
      statement.execute("FLUSH");

      for (int i = 1; i <= 3; i++) {
        for (int j = 0; j < 10; j++) {
          statement.execute(String.format(insertTemplate, i, j, j, j * 0.1, j));
        }
      }
      statement.execute("FLUSH root.group1");
      statement.execute("FLUSH root.group2,root.group3");

      for (int i = 1; i <= 3; i++) {
        for (int j = 0; j < 30; j++) {
          statement.execute(String.format(insertTemplate, i, j, j, j * 0.1, j));
        }
      }
      statement.execute("FLUSH root.group1 TRUE");
      statement.execute("FLUSH root.group2,root.group3 FALSE");

      int i = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT * FROM root.group1,root.group2,root" + ".group3")) {
        while (resultSet.next()) {
          i++;
        }
      }
      assertEquals(30, i);

    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFlushGivenGroupNoData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.nodatagroup1");
      statement.execute("CREATE DATABASE root.nodatagroup2");
      statement.execute("CREATE DATABASE root.nodatagroup3");
      statement.execute("FLUSH root.nodatagroup1");
      statement.execute("FLUSH root.nodatagroup2");
      statement.execute("FLUSH root.nodatagroup3");
      statement.execute("FLUSH root.nodatagroup1, root.nodatagroup2");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  @Ignore
  public void testFlushNotExistGroupNoData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.noexist.nodatagroup1");
      try {
        statement.execute(
            "FLUSH root.noexist.nodatagroup1,root.notExistGroup1,root.notExistGroup2");
      } catch (SQLException sqe) {
        String expectedMsg =
            "322: 322: storageGroup root.notExistGroup1,root.notExistGroup2 does not exist";
        sqe.printStackTrace();
        assertTrue(sqe.getMessage().contains(expectedMsg));
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
