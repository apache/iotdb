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
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBFlushQueryTableIT {

  private static String[] sqls =
      new String[] {
        "CREATE DATABASE test",
        "USE \"test\"",
        "CREATE TABLE vehicle (id1 string id, s0 int32 measurement)",
        "insert into vehicle(id1,time,s0) values('d0',1,101)",
        "insert into vehicle(id1,time,s0) values('d0',2,198)",
        "insert into vehicle(id1,time,s0) values('d0',100,99)",
        "insert into vehicle(id1,time,s0) values('d0',101,99)",
        "insert into vehicle(id1,time,s0) values('d0',102,80)",
        "insert into vehicle(id1,time,s0) values('d0',103,99)",
        "insert into vehicle(id1,time,s0) values('d0',104,90)",
        "insert into vehicle(id1,time,s0) values('d0',105,99)",
        "insert into vehicle(id1,time,s0) values('d0',106,99)",
        "flush",
        "insert into vehicle(id1,time,s0) values('d0',2,10000)",
        "insert into vehicle(id1,time,s0) values('d0',50,10000)",
        "insert into vehicle(id1,time,s0) values('d0',1000,22222)",
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
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        System.out.println(sql);
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }

  @Test
  public void selectAllSQLTest() {

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE \"test\"");
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM vehicle"); ) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFlushGivenGroup() {
    String insertTemplate =
        "INSERT INTO vehicle(id1, time, s1, s2, s3) VALUES (%s, %d, %d, %f, %s)";
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE group1");
      statement.execute("CREATE DATABASE group2");
      statement.execute("CREATE DATABASE group3");

      for (int i = 1; i <= 3; i++) {
        statement.execute(String.format("USE \"group%d\"", i));
        statement.execute(
            "CREATE TABLE vehicle (id1 string id, s1 int32 measurement, s2 float measurement, s3 string measurement)");
        for (int j = 10; j < 20; j++) {
          statement.execute(String.format(Locale.CHINA, insertTemplate, i, j, j, j * 0.1, j));
        }
      }
      statement.execute("FLUSH");

      for (int i = 1; i <= 3; i++) {
        statement.execute(String.format("USE \"group%d\"", i));
        for (int j = 0; j < 10; j++) {
          statement.execute(String.format(Locale.CHINA, insertTemplate, i, j, j, j * 0.1, j));
        }
      }
      statement.execute("FLUSH group1");
      statement.execute("FLUSH group2,group3");

      for (int i = 1; i <= 3; i++) {
        statement.execute(String.format("USE \"group%d\"", i));
        for (int j = 0; j < 30; j++) {
          statement.execute(String.format(Locale.CHINA, insertTemplate, i, j, j, j * 0.1, j));
        }
      }
      statement.execute("FLUSH group1 TRUE");
      statement.execute("FLUSH group2,group3 FALSE");

      for (int i = 1; i <= 3; i++) {
        statement.execute(String.format("USE \"group%d\"", i));
        int count = 0;
        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM vehicle")) {
          while (resultSet.next()) {
            count++;
          }
        }
        assertEquals(30, count);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testFlushGivenGroupNoData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE nodatagroup1");
      statement.execute("CREATE DATABASE nodatagroup2");
      statement.execute("CREATE DATABASE nodatagroup3");
      statement.execute("FLUSH nodatagroup1");
      statement.execute("FLUSH nodatagroup2");
      statement.execute("FLUSH nodatagroup3");
      statement.execute("FLUSH nodatagroup1, nodatagroup2");
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
