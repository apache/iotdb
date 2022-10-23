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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBDeactivateTemplateIT {

  private Statement statement;
  private Connection connection;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
    connection = EnvFactory.getEnv().getConnection();
    statement = connection.createStatement();

    prepareTemplate();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvFactory.getEnv().cleanAfterTest();
  }

  private void prepareTemplate() throws SQLException {
    // create storage group
    statement.execute("CREATE STORAGE GROUP root.sg1");
    statement.execute("CREATE STORAGE GROUP root.sg2");
    statement.execute("CREATE STORAGE GROUP root.sg3");
    statement.execute("CREATE STORAGE GROUP root.sg4");

    // create schema template
    statement.execute("CREATE SCHEMA TEMPLATE t1 (s1 INT64, s2 DOUBLE)");
    statement.execute("CREATE SCHEMA TEMPLATE t2 (s1 INT64, s2 DOUBLE)");

    // set schema template
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg1");
    statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg2");
    statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg3");
    statement.execute("SET SCHEMA TEMPLATE t2 TO root.sg4");

    String insertSql = "insert into root.sg%d.d1(time, s1, s2) values(%d, %d, %d)";
    for (int i = 1; i <= 4; i++) {
      for (int j = 1; j <= 4; j++) {
        statement.execute(String.format(insertSql, j, i, i, i));
      }
    }
  }

  @Test
  public void deactivateTemplateAndReactivateTest() throws Exception {
    statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg1.d1");
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertFalse(resultSet.next());
    }

    statement.execute("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg1.d1");

    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(3, resultSetMetaData.getColumnCount());
      Assert.assertFalse(resultSet.next());
    }

    statement.execute("insert into root.sg1.d1(time, s1, s2) values(1, 1, 1)");

    String[] retArray = new String[] {"1,1,1.0,"};
    int cnt = 0;
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(3, resultSetMetaData.getColumnCount());
      while (resultSet.next()) {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          builder.append(resultSet.getString(i)).append(",");
        }
        Assert.assertEquals(retArray[cnt], builder.toString());
        cnt++;
      }
      Assert.assertEquals(1, cnt);
    }
  }

  @Test
  public void deactivateTemplateAndAutoDeleteDeviceTest() throws Exception {
    statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg1.d1");
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertFalse(resultSet.next());
    }
    try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES root.sg1.*")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void deactivateTemplateCrossSchemaRegionTest() throws Exception {
    String insertSql = "insert into root.sg1.d%d(time, s1, s2) values(%d, %d, %d)";
    for (int i = 1; i <= 4; i++) {
      for (int j = 1; j <= 4; j++) {
        statement.execute(String.format(insertSql, j, i, i, i));
      }
    }

    statement.execute("DEACTIVATE TEMPLATE FROM root.sg1.*");
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.**")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void deactivateTemplateCrossStorageGroupTest() throws Exception {
    String insertSql = "insert into root.sg%d.d2(time, s1, s2) values(%d, %d, %d)";
    for (int i = 1; i <= 4; i++) {
      for (int j = 1; j <= 2; j++) {
        statement.execute(String.format(insertSql, j, i, i, i));
      }
    }

    statement.execute("DEACTIVATE TEMPLATE FROM root.*.d1");
    String[] retArray =
        new String[] {"1,1,1.0,1,1.0,", "2,2,2.0,2,2.0,", "3,3,3.0,3,3.0,", "4,4,4.0,4,4.0,"};
    int cnt = 0;
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(5, resultSetMetaData.getColumnCount());
      while (resultSet.next()) {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          builder.append(resultSet.getString(i)).append(",");
        }
        Assert.assertEquals(retArray[cnt], builder.toString());
        cnt++;
      }
      Assert.assertEquals(retArray.length, cnt);
    }

    statement.execute("DEACTIVATE TEMPLATE FROM root.**, root.sg1.*");
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void deactivateTemplateWithMultiPatternTest() throws Exception {
    statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg1.d1, root.sg2.*");
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*, root.sg2.*")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void deactivateNoneUsageTemplateTest() throws Exception {
    statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg5.d1");

    statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg1.d1");

    statement.execute("DEACTIVATE SCHEMA TEMPLATE t1 FROM root.sg1.d1");
  }

  @Test
  public void multiSyntaxTest() throws Exception {
    statement.execute("DELETE TIMESERIES OF SCHEMA TEMPLATE t1 FROM root.sg1.d1");
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg1.*")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertFalse(resultSet.next());
    }

    statement.execute("DEACTIVATE SCHEMA TEMPLATE t2 FROM root.sg3.d1");
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.sg3.*")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertFalse(resultSet.next());
    }

    statement.execute("DEACTIVATE TEMPLATE FROM root.**");
    try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**")) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Assert.assertEquals(1, resultSetMetaData.getColumnCount());
      Assert.assertFalse(resultSet.next());
    }
  }
}
