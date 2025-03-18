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
        "CREATE TABLE vehicle (tag1 string tag, s0 int32 field)",
        "insert into vehicle(tag1,time,s0) values('d0',1,101)",
        "insert into vehicle(tag1,time,s0) values('d0',2,198)",
        "insert into vehicle(tag1,time,s0) values('d0',100,99)",
        "insert into vehicle(tag1,time,s0) values('d0',101,99)",
        "insert into vehicle(tag1,time,s0) values('d0',102,80)",
        "insert into vehicle(tag1,time,s0) values('d0',103,99)",
        "insert into vehicle(tag1,time,s0) values('d0',104,90)",
        "insert into vehicle(tag1,time,s0) values('d0',105,99)",
        "insert into vehicle(tag1,time,s0) values('d0',106,99)",
        "flush",
        "insert into vehicle(tag1,time,s0) values('d0',2,10000)",
        "insert into vehicle(tag1,time,s0) values('d0',50,10000)",
        "insert into vehicle(tag1,time,s0) values('d0',1000,22222)",
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
        "INSERT INTO vehicle(tag1, time, s1, s2, s3) VALUES (%s, %d, %d, %f, %s)";
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE group1");
      statement.execute("CREATE DATABASE group2");
      statement.execute("CREATE DATABASE group3");

      for (int i = 1; i <= 3; i++) {
        statement.execute(String.format("USE \"group%d\"", i));
        statement.execute(
            "CREATE TABLE vehicle (tag1 string tag, s1 int32 field, s2 float field, s3 string field)");
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
  public void testFlushNotExistGroupNoData() {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE noexist_nodatagroup1");
      try {
        statement.execute("FLUSH noexist_nodatagroup1,notExistGroup1,notExistGroup2");
      } catch (final SQLException sqe) {
        String expectedMsg = "Database notExistGroup1,notExistGroup2 does not exist";
        assertTrue(sqe.getMessage().contains(expectedMsg));
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFlushTableAfterDropColumn() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database db1");
      statement.execute("use db1");
      statement.execute("create table t2(s1 text field, s2 text field)");
      statement.execute("insert into t2(time,s2) values(2,'t1')");
      statement.execute("alter table t2 drop column s2");
      statement.execute("flush");
      statement.execute("insert into t2(time,s1) values(2,'t1')");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
