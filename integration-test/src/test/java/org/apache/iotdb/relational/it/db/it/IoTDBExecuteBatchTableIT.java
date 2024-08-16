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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.itbase.env.BaseEnv.TABLE_SQL_DIALECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
@Ignore // 'Drop Table' and 'Alter table' is not supported
public class IoTDBExecuteBatchTableIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testJDBCExecuteBatch() {

    try (Connection connection = EnvFactory.getEnv().getConnection(TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.addBatch("create database ln");
      statement.addBatch("USE \"ln\"");
      statement.addBatch("create table wf01 (id1 string id, temprature double measurement)");
      statement.addBatch(
          "insert into wf01(id1,time,temperature) values(\'wt01\', 1509465600000,1.2)");
      statement.addBatch(
          "insert into wf01(id1,time,temperature) values(\'wt01\', 1509465600001,2.3)");

      statement.addBatch("drop table wf01");
      statement.addBatch("create table wf01 (id1 string id, temprature double measurement)");

      statement.addBatch(
          "insert into wf01(id1,time,temperature) values(\'wt01\', 1509465600002,3.4)");
      statement.executeBatch();
      statement.clearBatch();
      ResultSet resultSet = statement.executeQuery("select * from wf01");
      int count = 0;

      String[] timestamps = {"1509465600002"};
      String[] values = {"3.4"};

      while (resultSet.next()) {
        assertEquals(timestamps[count], resultSet.getString("time"));
        assertEquals(values[count], resultSet.getString("temperature"));
        count++;
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testJDBCExecuteBatchForCreateMultiTimeSeriesPlan() {
    try (Connection connection = EnvFactory.getEnv().getConnection(TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(100);
      statement.execute("create database ln");
      statement.execute("USE \"ln\"");
      statement.addBatch("create table wf01 (id1 string id, temprature double measurement)");

      statement.addBatch(
          "insert into wf01(id1,time,temperature) values(\'wt01\', 1509465600000,1.2)");
      statement.addBatch(
          "insert into wf01(id1,time,temperature) values(\'wt01\', 1509465600001,2.3)");
      statement.addBatch("drop table wf01");

      statement.addBatch(
          "create table turbine (id1 string id, attr1 string attribute, attr2 string attribute, s1 boolean measurement, s2 float measurement)");

      statement.addBatch("create table wf01 (id1 string id, temprature double measurement)");
      statement.addBatch(
          "insert into wf01(id1,time,temperature) values(\'wt01\', 1509465600002,3.4)");
      statement.addBatch("alter table turbine add column s3 boolean measurement");
      statement.executeBatch();
      statement.clearBatch();
      ResultSet resultSet = statement.executeQuery("select * from wf01");
      String[] timestamps = {"1509465600002"};
      String[] values = {"3.4"};
      int count = 0;
      while (resultSet.next()) {
        assertEquals(timestamps[count], resultSet.getString("time"));
        assertEquals(values[count], resultSet.getString("temperature"));
        count++;
      }
      ResultSet timeSeriesResultSetForS1 = statement.executeQuery("describe turbine");
      count = 0;
      String[] keys = {"ColumnName", "DataType", "Category"};
      String[][] value_columns = {
        new String[] {"Time", "TIMESTAMP", "TIME"},
        new String[] {"id1", "STRING", "ID"},
        new String[] {"attr1", "STRING", "ATTRIBUTE"},
        new String[] {"attr2", "STRING", "ATTRIBUTE"},
        new String[] {"s1", "BOOLEAN", "MEASUREMENT"},
        new String[] {"s2", "FLOAT", "MEASUREMENT"},
      };

      while (timeSeriesResultSetForS1.next()) {
        for (int i = 0; i < keys.length; i++) {
          assertEquals(value_columns[count][i], timeSeriesResultSetForS1.getString(keys[i]));
        }
        count++;
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }
}
