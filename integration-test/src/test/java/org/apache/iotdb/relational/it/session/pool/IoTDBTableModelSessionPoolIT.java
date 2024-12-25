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

package org.apache.iotdb.relational.it.session.pool;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showTablesColumnHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableModelSessionPoolIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testUseDatabase() {

    final String[] table1Names = new String[] {"table1"};
    final String[] table1ttls = new String[] {"3600000"};

    final String[] table2Names = new String[] {"table2"};
    final String[] table2ttls = new String[] {"6600000"};

    ITableSessionPool sessionPool = EnvFactory.getEnv().getTableSessionPool(1);
    try (final ITableSession session = sessionPool.getSession()) {

      session.executeNonQueryStatement("CREATE DATABASE test1");
      session.executeNonQueryStatement("CREATE DATABASE test2");

      session.executeNonQueryStatement("use test2");

      // or use full qualified table name
      session.executeNonQueryStatement(
          "create table test1.table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");

      session.executeNonQueryStatement(
          "create table table2(region_id STRING ID, plant_id STRING ID, color STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, speed DOUBLE MEASUREMENT) with (TTL=6600000)");

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          final RowRecord rowRecord = dataSet.next();
          assertEquals(table2Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table2ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table2Names.length, cnt);
      }

    } catch (final IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (final ITableSession session = sessionPool.getSession()) {
      // current session's database is still test2
      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          RowRecord rowRecord = dataSet.next();
          assertEquals(table2Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table2ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table2Names.length, cnt);
      }

    } catch (final IoTDBConnectionException | StatementExecutionException e) {
      fail(e.getMessage());
    } finally {
      sessionPool.close();
    }

    // specify database in constructor
    sessionPool = EnvFactory.getEnv().getTableSessionPool(1, "test1");

    try (final ITableSession session = sessionPool.getSession()) {

      // current session's database is test1
      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          RowRecord rowRecord = dataSet.next();
          assertEquals(table1Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table1ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table1Names.length, cnt);
      }

      // change database to test2
      session.executeNonQueryStatement("use test2");

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          RowRecord rowRecord = dataSet.next();
          assertEquals(table2Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table2ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table2Names.length, cnt);
      }

    } catch (final IoTDBConnectionException | StatementExecutionException e) {
      fail(e.getMessage());
    }

    // after putting back, the session's database should be changed back to default test1
    try (final ITableSession session = sessionPool.getSession()) {

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          RowRecord rowRecord = dataSet.next();
          assertEquals(table1Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table1ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table1Names.length, cnt);
      }

    } catch (final IoTDBConnectionException | StatementExecutionException e) {
      fail(e.getMessage());
    } finally {
      sessionPool.close();
    }
  }
}
