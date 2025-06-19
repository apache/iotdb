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

package org.apache.iotdb.relational.it.query.view.recent;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableViewWithTreeTTLQueryIT {

  protected static final String DATABASE_NAME = "test";

  protected static String[] createTreeDataSqls = {
    "CREATE ALIGNED TIMESERIES root.db.battery.b0(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b0(time, voltage, current) aligned values ("
        + (System.currentTimeMillis() - 100000)
        + ", 1, 1)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b1(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (1, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (2, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (3, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (4, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values ("
        + System.currentTimeMillis()
        + ", 1, 1)",
    "CREATE TIMESERIES root.db.battery.b2.voltage INT32",
    "CREATE TIMESERIES root.db.battery.b2.current FLOAT",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (1, 1, 1)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (2, 1, 1)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (3, 1, 1)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (4, 1, 1)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values ("
        + System.currentTimeMillis()
        + ", 1, 1)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b3(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b3(time, voltage, current) aligned values ("
        + (System.currentTimeMillis() - 100000)
        + ", 1, 1)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b4(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b4(time, voltage, current) aligned values ("
        + (System.currentTimeMillis() - 100000)
        + ", 1, 1)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b5(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b5(time, voltage, current) aligned values ("
        + (System.currentTimeMillis() - 100000)
        + ", 1, 1)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b6(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b6(time, voltage, current) aligned values ("
        + (System.currentTimeMillis() - 100000)
        + ", 1, 1)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b7(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b7(time, voltage, current) aligned values ("
        + (System.currentTimeMillis() - 100000)
        + ", 1, 1)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b8(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b8(time, voltage, current) aligned values ("
        + (System.currentTimeMillis() - 100000)
        + ", 1, 1)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b9(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b9(time, voltage, current) aligned values ("
        + (System.currentTimeMillis() - 100000)
        + ", 1, 1)",
    "flush",
    "set ttl to root.db.battery.** 200000",
    "set ttl to root.db.battery.b0 50000",
    "set ttl to root.db.battery.b6 50000",
  };

  protected static String[] createTableSqls = {
    "CREATE DATABASE " + DATABASE_NAME,
    "USE " + DATABASE_NAME,
    "CREATE VIEW view1 (battery TAG, voltage INT32 FIELD, current FLOAT FIELD) as root.db.battery.**",
  };

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(createTreeDataSqls);
    prepareTableData(createTableSqls);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + DATABASE_NAME);
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select count(*) from view1 where battery = 'b1'");
      Assert.assertTrue(sessionDataSet.hasNext());
      RowRecord record = sessionDataSet.next();
      Assert.assertEquals(1, record.getField(0).getLongV());
      Assert.assertFalse(sessionDataSet.hasNext());
      sessionDataSet.close();

      sessionDataSet = session.executeQueryStatement("select * from view1");
      int count = 0;
      while (sessionDataSet.hasNext()) {
        sessionDataSet.next();
        count++;
      }
      sessionDataSet.close();
      Assert.assertEquals(8, count);
    }
  }
}
