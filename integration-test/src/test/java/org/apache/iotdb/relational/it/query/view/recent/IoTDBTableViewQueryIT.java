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
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.apache.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;

@RunWith(Parameterized.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableViewQueryIT {

  protected static final String DATABASE_NAME = "test";

  protected static String[] createTreeAlignedDataSqls = {
    "CREATE ALIGNED TIMESERIES root.db.battery.b1(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (1, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (2, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (3, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (4, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) aligned values (5, 1, 1)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b2(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) aligned values (1, 2, 2)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) aligned values (2, 2, 2)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) aligned values (3, 2, 2)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) aligned values (4, 2, 2)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) aligned values (5, null, 2)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b3(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b3(time, voltage, current) aligned values (1, 3, 3)",
    "INSERT INTO root.db.battery.b3(time, voltage, current) aligned values (2, null, 3)",
    "INSERT INTO root.db.battery.b3(time, voltage, current) aligned values (3, 3, 3)",
    "INSERT INTO root.db.battery.b3(time, voltage, current) aligned values (4, 3, 3)",
    "CREATE ALIGNED TIMESERIES root.db.battery.b4(voltage INT32, current FLOAT)",
    "INSERT INTO root.db.battery.b4(time, voltage, current) aligned values (1, 4, 4)",
    "INSERT INTO root.db.battery.b4(time, voltage, current) aligned values (2, 4, 4)",
    "INSERT INTO root.db.battery.b4(time, voltage, current) aligned values (3, 4, null)",
    "INSERT INTO root.db.battery.b4(time, voltage, current) aligned values (4, 4, 4)",
  };
  protected static String[] createTreeNonAlignedDataSqls = {
    "CREATE TIMESERIES root.db.battery.b1.voltage INT32",
    "CREATE TIMESERIES root.db.battery.b1.current FLOAT",
    "INSERT INTO root.db.battery.b1(time, voltage, current) values (1, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) values (2, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) values (3, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) values (4, 1, 1)",
    "INSERT INTO root.db.battery.b1(time, voltage, current) values (5, 1, 1)",
    "CREATE TIMESERIES root.db.battery.b2.voltage INT32",
    "CREATE TIMESERIES root.db.battery.b2.current FLOAT",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (1, 2, 2)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (2, 2, 2)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (3, 2, 2)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (4, 2, 2)",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (5, null, 2)",
    "CREATE TIMESERIES root.db.battery.b3.voltage INT32",
    "CREATE TIMESERIES root.db.battery.b3.current FLOAT",
    "INSERT INTO root.db.battery.b3(time, voltage, current) values (1, 3, 3)",
    "INSERT INTO root.db.battery.b3(time, voltage, current) values (2, null, 3)",
    "INSERT INTO root.db.battery.b3(time, voltage, current) values (3, 3, 3)",
    "INSERT INTO root.db.battery.b3(time, voltage, current) values (4, 3, 3)",
    "CREATE TIMESERIES root.db.battery.b4.voltage INT32",
    "CREATE TIMESERIES root.db.battery.b4.current FLOAT",
    "INSERT INTO root.db.battery.b4(time, voltage, current) values (1, 4, 4)",
    "INSERT INTO root.db.battery.b4(time, voltage, current) values (2, 4, 4)",
    "INSERT INTO root.db.battery.b4(time, voltage, current) values (3, 4, null)",
    "INSERT INTO root.db.battery.b4(time, voltage, current) values (4, 4, 4)",
  };

  protected static String[] createTableSqls = {
    "CREATE DATABASE " + DATABASE_NAME,
    "USE " + DATABASE_NAME,
    "CREATE VIEW view1 (battery TAG, voltage INT32 FIELD, current FLOAT FIELD) as root.db.battery.**",
    "CREATE VIEW view2 (battery TAG, voltage INT32 FIELD FROM voltage, current_rename FLOAT FIELD FROM current) as root.db.battery.**",
    "CREATE VIEW view3 (battery TAG, voltage INT32 FIELD FROM voltage, current_rename FLOAT FIELD FROM current) with (ttl=1) as root.db.battery.**",
    "CREATE TABLE table1 (battery TAG, voltage INT32 FIELD, current FLOAT FIELD)",
    "INSERT INTO table1 (time, battery, voltage, current) values (1, 'b1', 1, 1)",
    "INSERT INTO table1 (time, battery, voltage, current) values (2, 'b1', 1, 1)",
    "INSERT INTO table1 (time, battery, voltage, current) values (3, 'b1', 1, 1)",
    "INSERT INTO table1 (time, battery, voltage, current) values (4, 'b1', 1, 1)",
    "INSERT INTO table1 (time, battery, voltage, current) values (5, 'b1', 1, 1)",
    "INSERT INTO table1 (time, battery, voltage, current) values (1, 'b2', 2, 2)",
    "INSERT INTO table1 (time, battery, voltage, current) values (2, 'b2', 2, 2)",
    "INSERT INTO table1 (time, battery, voltage, current) values (3, 'b2', 2, 2)",
    "INSERT INTO table1 (time, battery, voltage, current) values (4, 'b2', 2, 2)",
    "INSERT INTO table1 (time, battery, voltage, current) values (5, 'b2', null, 2)",
    "INSERT INTO table1 (time, battery, voltage, current) values (1, 'b3', 3, 3)",
    "INSERT INTO table1 (time, battery, voltage, current) values (2, 'b3', null, 3)",
    "INSERT INTO table1 (time, battery, voltage, current) values (3, 'b3', 3, 3)",
    "INSERT INTO table1 (time, battery, voltage, current) values (4, 'b3', 3, 3)",
    "INSERT INTO table1 (time, battery, voltage, current) values (1, 'b4', 4, 4)",
    "INSERT INTO table1 (time, battery, voltage, current) values (2, 'b4', 4, 4)",
    "INSERT INTO table1 (time, battery, voltage, current) values (3, 'b4', 4, null)",
    "INSERT INTO table1 (time, battery, voltage, current) values (4, 'b4', 4, 4)",
  };

  @Parameterized.Parameters(name = "aligned={0}, flush={1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {{true, true}, {false, true}, {true, false}, {false, false}});
  }

  private final boolean aligned;
  private final boolean flush;

  public IoTDBTableViewQueryIT(boolean aligned, boolean flush) {
    this.aligned = aligned;
    this.flush = flush;
  }

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(aligned ? createTreeAlignedDataSqls : createTreeNonAlignedDataSqls);
    if (flush) {
      prepareData(new String[] {"flush"});
    }
    prepareTableData(createTableSqls);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      compareQueryResults(session, "select * from view1", "select * from table1", true);
      compareQueryResults(
          session,
          "select time, battery, current, voltage from view1",
          "select time, battery, current_rename, voltage from view2",
          true);
      compareQueryResults(session, "select battery from view1", "select battery from table1", true);
      compareQueryResults(
          session,
          "select current from view1 where time > 1",
          "select current from table1 where time > 1" + (aligned ? "" : " and current is not null"),
          true);
      compareQueryResults(
          session,
          "select * from view1 fill method linear",
          "select * from table1 fill method linear",
          true);
      compareQueryResults(
          session,
          "select * from view1 fill method previous",
          "select * from table1 fill method previous",
          true);
      compareQueryResults(
          session,
          "select * from view1 fill method constant 0",
          "select * from table1 fill method constant 0",
          true);
      compareQueryResults(
          session,
          "select * from view1 where time > 1",
          "select * from table1 where time > 1",
          true);
      compareQueryResults(
          session,
          "select * from view1 where time > 1 and voltage > 1",
          "select * from table1 where time > 1 and voltage > 1",
          true);
      compareQueryResults(
          session,
          "select * from view1 where time > 1 and (voltage + current) > 1",
          "select * from table1 where time > 1 and (voltage + current) > 1",
          true);
      compareQueryResults(
          session,
          "select * from view1 where voltage is null",
          "select * from table1 where voltage is null",
          true);
      compareQueryResults(
          session,
          "select * from view1 where current is not null",
          "select * from table1 where current is not null",
          true);
      compareQueryResults(
          session,
          "select * from view1 where current > 1 and voltage > 3",
          "select * from table1 where current > 1 and voltage > 3",
          true);
      compareQueryResults(
          session,
          "select * from view1 where current > 1 or voltage > 3",
          "select * from table1 where current > 1 or voltage > 3",
          true);
      compareQueryResults(
          session,
          "select * from view1 where battery='b1' limit 1",
          "select * from table1 where battery='b1' limit 1",
          true);
      compareQueryResults(
          session,
          "select * from view1 where battery='b1' offset 1 limit 1",
          "select * from table1 where battery='b1' offset 1 limit 1",
          true);
      compareQueryResults(
          session,
          "select * from view1 where battery='b1' offset 5 limit 1",
          "select * from table1 where battery='b1' offset 5 limit 1",
          true);
      compareQueryResults(
          session,
          "select * from view1 order by battery, time limit 1",
          "select * from table1 order by battery, time limit 1",
          false);
      compareQueryResults(
          session,
          "select * from view1 order by battery, time offset 2 limit 2",
          "select * from table1 order by battery, time offset 2 limit 2",
          false);
      compareQueryResults(
          session,
          "select * from view1 order by battery, time asc limit 1",
          "select * from table1 order by battery, time limit 1",
          false);
      compareQueryResults(
          session,
          "select * from view1 order by battery, time asc offset 2 limit 2",
          "select * from table1 order by battery, time offset 2 limit 2",
          false);

      compareQueryResults(
          session,
          "select time, battery, current from view1",
          "select time, battery, current from table1"
              + (aligned ? "" : " where current is not null"),
          true);
      compareQueryResults(
          session,
          "select time, battery, current from view1 where voltage > 1",
          "select time, battery, current from table1 where voltage > 1",
          true);
      compareQueryResults(
          session,
          "select time, battery, current from view1 where voltage > 1 and current > 3",
          "select time, battery, current from table1 where voltage > 1 and current > 3",
          true);
      compareQueryResults(
          session,
          "select time, battery, current from view1 where voltage > 1 or current > 0",
          "select time, battery, current from table1 where voltage > 1 or current > 0",
          true);

      compareQueryResults(
          session, "select count(*) from view1", "select count(*) from table1", true);
      compareQueryResults(
          session, "select count(battery) from view1", "select count(battery) from table1", true);
      compareQueryResults(
          session,
          "select count(*) from view1 where time = 1",
          "select count(*) from table1 where time = 1",
          true);
      compareQueryResults(
          session,
          "select count(*) from view1 group by battery",
          "select count(*) from table1 group by battery",
          true);
      compareQueryResults(
          session,
          "select count(*) from view1 where time > 1 group by battery",
          "select count(*) from table1 where time > 1 group by battery",
          true);

      compareQueryResults(
          session, "select avg(current) from view1", "select avg(current) from table1", true);
      compareQueryResults(
          session,
          "select count(*) from view1 where time = 1",
          "select count(*) from table1 where time = 1",
          true);
      compareQueryResults(
          session,
          "select count(*) from view1 group by battery",
          "select count(*) from table1 group by battery",
          true);
      compareQueryResults(
          session,
          "select count(*) from view1 where time > 1 group by battery",
          "select count(*) from table1 where time > 1 group by battery",
          true);
      compareQueryResults(
          session,
          "select count(*) from view1 group by battery having count(*) >= 5",
          "select count(*) from table1 group by battery having count(*) >= 5",
          true);

      compareQueryResults(
          session,
          "select current from view1 where current >= (select avg(current) from view1)",
          "select current from table1 where current >= (select avg(current) from table1)",
          true);

      compareQueryResults(
          session,
          "select current from view1 where battery='b1' and current >= (select avg(current) from view1 where battery='b1')",
          "select current from table1 where battery='b1' and current >= (select avg(current) from table1 where battery='b1')",
          true);
      // empty result
      compareQueryResults(
          session, "select * from view3 limit 1", "select * from table1 limit 0", true);

      // not exists
      compareQueryResults(
          session,
          "select count(*) from view1 where battery = 'b'",
          "select count(*) from table1 where battery = 'b'",
          false);
      compareQueryResults(
          session,
          "select * from (select time, battery as device1 from view1 where battery = 'b1') as t1 full outer join (select time, battery as device2 from view2 where battery = 'b') as t2 using(time)",
          "select * from (select time, battery as device1 from table1 where battery = 'b1') as t1 full outer join (select time, battery as device2 from table1 where battery = 'b') as t2 using(time)",
          true);
      compareQueryResults(
          session,
          "select * from (select * from view1 where battery = 'b1') join (select * from view1 where battery = 'b1' and (voltage > 0 or current > 0)) using(time)",
          "select * from (select * from table1 where battery = 'b1') join (select * from table1 where battery = 'b1' and (voltage > 0 or current > 0)) using(time)",
          true);
    }
  }

  private static void compareQueryResults(
      ITableSession session, String sql1, String sql2, boolean sort) throws Exception {
    List<String> records1 = new ArrayList<>();
    List<String> records2 = new ArrayList<>();
    session.executeNonQueryStatement("USE " + DATABASE_NAME);
    SessionDataSet sessionDataSet = session.executeQueryStatement(sql1);
    while (sessionDataSet.hasNext()) {
      RowRecord record = sessionDataSet.next();
      records1.add(record.toString());
    }
    sessionDataSet.close();
    sessionDataSet = session.executeQueryStatement(sql2);
    while (sessionDataSet.hasNext()) {
      RowRecord record = sessionDataSet.next();
      records2.add(record.toString());
    }
    sessionDataSet.close();
    if (sort) {
      records1.sort(String::compareTo);
      records2.sort(String::compareTo);
    }
    Assert.assertEquals(records1, records2);
  }
}
