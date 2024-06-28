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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.describeTableColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.showTablesColumnHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testManageDatabase() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("create database test1");
      statement.execute("create database test2");

      // should specify database before create table
      try {
        statement.execute(
            "create table table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
      } catch (SQLException e) {
        assertEquals("701: database is not specified", e.getMessage());
      }

      // or use full qualified table name
      statement.execute(
          "create table test1.table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");

      statement.execute("use test2");

      try {
        statement.execute(
            "create table table2(region_id TEXT ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
      } catch (SQLException e) {
        assertEquals(
            "701: DataType of ID Column should only be STRING, current is TEXT", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id INT32 ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
      } catch (SQLException e) {
        assertEquals(
            "701: DataType of ID Column should only be STRING, current is INT32", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model TEXT ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
      } catch (SQLException e) {
        assertEquals(
            "701: DataType of ATTRIBUTE Column should only be STRING, current is TEXT",
            e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model DOUBLE ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
      } catch (SQLException e) {
        assertEquals(
            "701: DataType of ATTRIBUTE Column should only be STRING, current is DOUBLE",
            e.getMessage());
      }

      statement.execute(
          "create table table2(region_id STRING ID, plant_id STRING ID, color STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, speed DOUBLE MEASUREMENT) with (TTL=6600000)");

      String[] tableNames = new String[] {"table2"};
      long[] ttls = new long[] {6600000L};

      // show tables from current database
      try (ResultSet resultSet = statement.executeQuery("SHOW tables")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(tableNames[cnt], resultSet.getString(1));
          assertEquals(ttls[cnt], resultSet.getInt(2));
          cnt++;
        }
        assertEquals(tableNames.length, cnt);
      }

      // show tables by specifying another database
      tableNames = new String[] {"table1"};
      ttls = new long[] {3600000L};

      // using SHOW tables in
      try (ResultSet resultSet = statement.executeQuery("SHOW tables in test1")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(tableNames[cnt], resultSet.getString(1));
          assertEquals(ttls[cnt], resultSet.getInt(2));
          cnt++;
        }
        assertEquals(tableNames.length, cnt);
      }

      // using SHOW tables from
      try (ResultSet resultSet = statement.executeQuery("SHOW tables from test1")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(tableNames[cnt], resultSet.getString(1));
          assertEquals(ttls[cnt], resultSet.getInt(2));
          cnt++;
        }
        assertEquals(tableNames.length, cnt);
      }

      // show tables from a non-exist database
      try {
        statement.executeQuery("SHOW tables from test3");
      } catch (SQLException e) {
        assertEquals("500: Database test3 doesn't exists.", e.getMessage());
      }

      // describe
      try {
        statement.executeQuery("describe table1");
      } catch (SQLException e) {
        assertEquals("550: Table test2.table1 not exists.", e.getMessage());
      }

      String[] columnNames =
          new String[] {
            "Time", "region_id", "plant_id", "device_id", "model", "temperature", "humidity"
          };
      String[] dataTypes =
          new String[] {"INT64", "STRING", "STRING", "STRING", "STRING", "FLOAT", "DOUBLE"};
      String[] categories =
          new String[] {"TIME", "ID", "ID", "ID", "ATTRIBUTE", "MEASUREMENT", "MEASUREMENT"};

      try (ResultSet resultSet = statement.executeQuery("describe test1.table1")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(describeTableColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < describeTableColumnHeaders.size(); i++) {
          assertEquals(
              describeTableColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(columnNames[cnt], resultSet.getString(1));
          assertEquals(dataTypes[cnt], resultSet.getString(2));
          assertEquals(categories[cnt], resultSet.getString(3));
          cnt++;
        }
        assertEquals(columnNames.length, cnt);
      }

      columnNames = new String[] {"Time", "region_id", "plant_id", "color", "temperature", "speed"};
      dataTypes = new String[] {"INT64", "STRING", "STRING", "STRING", "FLOAT", "DOUBLE"};
      categories = new String[] {"TIME", "ID", "ID", "ATTRIBUTE", "MEASUREMENT", "MEASUREMENT"};

      try (ResultSet resultSet = statement.executeQuery("desc table2")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(describeTableColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < describeTableColumnHeaders.size(); i++) {
          assertEquals(
              describeTableColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(columnNames[cnt], resultSet.getString(1));
          assertEquals(dataTypes[cnt], resultSet.getString(2));
          assertEquals(categories[cnt], resultSet.getString(3));
          cnt++;
        }
        assertEquals(columnNames.length, cnt);
      }

      try {
        statement.executeQuery("describe test3.table3");
      } catch (SQLException e) {
        assertEquals("550: Table test3.table3 not exists.", e.getMessage());
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
