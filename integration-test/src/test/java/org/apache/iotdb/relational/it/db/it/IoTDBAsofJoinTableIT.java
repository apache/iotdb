/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAsofJoinTableIT {
  private static final String DATABASE_NAME = "test";

  private static final String[] sql =
      new String[] {
        "create database test",
        "use test",
        "create table table1(device string tag, value int32 field)",
        "insert into table1(time,device,value) values(2020-01-01 00:00:01.000,'d1',1)",
        "insert into table1(time,device,value) values(2020-01-01 00:00:03.000,'d1',3)",
        "insert into table1(time,device,value) values(2020-01-01 00:00:05.000,'d1',5)",
        "insert into table1(time,device,value) values(2020-01-01 00:00:08.000,'d2',8)",
        "create table table2(device string tag, value int32 field)",
        "insert into table2(time,device,value) values(2020-01-01 00:00:02.000,'d1',20)",
        "insert into table2(time,device,value) values(2020-01-01 00:00:03.000,'d1',30)",
        "insert into table2(time,device,value) values(2020-01-01 00:00:04.000,'d2',40)",
        "insert into table2(time,device,value) values(2020-01-01 00:00:05.000,'d2',50)"
      };
  String[] expectedHeader =
      new String[] {"time1", "device1", "value1", "time2", "device2", "value2"};
  ;
  String[] retArray;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setMaxTsBlockLineNumber(2)
        .setMaxNumberOfPointsInPage(5)
        .setDataNodeMemoryProportion("2:4:1:1:1:1")
        .setQueryMemoryProportion("1:100:100:10:400:200:100:50")
        .setSortBufferSize(1024 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sql) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void innerJoinTest() {
    retArray =
        new String[] {
          "2020-01-01T00:00:03.000Z,d1,3,2020-01-01T00:00:03.000Z,d1,30,",
          "2020-01-01T00:00:05.000Z,d1,5,2020-01-01T00:00:05.000Z,d2,50,",
          "2020-01-01T00:00:08.000Z,d2,8,2020-01-01T00:00:05.000Z,d2,50,"
        };
    tableResultSetEqualTest(
        "SELECT t1.time as time1, t1.device as device1, t1.value as value1, \n"
            + "       t2.time as time2, t2.device as device2, t2.value as value2 \n"
            + "FROM \n"
            + "table1 t1 ASOF INNER JOIN table2 t2\n"
            + "ON\n"
            + "t1.time>=t2.time\n"
            + "order by time1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "2020-01-01T00:00:03.000Z,d1,3,2020-01-01T00:00:03.000Z,d1,30,",
          "2020-01-01T00:00:05.000Z,d1,5,2020-01-01T00:00:05.000Z,d2,50,",
        };
    tableResultSetEqualTest(
        "SELECT t1.time as time1, t1.device as device1, t1.value as value1, \n"
            + "       t2.time as time2, t2.device as device2, t2.value as value2 \n"
            + "FROM \n"
            + "table1 t1 ASOF(tolerance 2s) INNER JOIN table2 t2\n"
            + "ON\n"
            + "t1.time>=t2.time\n"
            + "order by time1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "2020-01-01T00:00:03.000Z,d1,3,2020-01-01T00:00:03.000Z,d1,30,",
          "2020-01-01T00:00:05.000Z,d1,5,2020-01-01T00:00:03.000Z,d1,30,",
        };
    tableResultSetEqualTest(
        "SELECT t1.time as time1, t1.device as device1, t1.value as value1, \n"
            + "       t2.time as time2, t2.device as device2, t2.value as value2 \n"
            + "FROM \n"
            + "table1 t1 ASOF(tolerance 2s) INNER JOIN table2 t2\n"
            + "ON\n"
            + "t1.device=t2.device AND t1.time>=t2.time\n"
            + "order by time1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void leftJoinTest() {
    retArray =
        new String[] {
          "2020-01-01T00:00:01.000Z,d1,1,null,null,null,",
          "2020-01-01T00:00:03.000Z,d1,3,2020-01-01T00:00:03.000Z,d1,30,",
          "2020-01-01T00:00:05.000Z,d1,5,2020-01-01T00:00:05.000Z,d2,50,",
          "2020-01-01T00:00:08.000Z,d2,8,2020-01-01T00:00:05.000Z,d2,50,"
        };
    tableResultSetEqualTest(
        "SELECT t1.time as time1, t1.device as device1, t1.value as value1, \n"
            + "       t2.time as time2, t2.device as device2, t2.value as value2 \n"
            + "FROM \n"
            + "table1 t1 ASOF LEFT JOIN table2 t2\n"
            + "ON\n"
            + "t1.time>=t2.time\n"
            + "order by time1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "2020-01-01T00:00:01.000Z,d1,1,null,null,null,",
          "2020-01-01T00:00:03.000Z,d1,3,2020-01-01T00:00:03.000Z,d1,30,",
          "2020-01-01T00:00:05.000Z,d1,5,2020-01-01T00:00:03.000Z,d1,30,",
          "2020-01-01T00:00:08.000Z,d2,8,2020-01-01T00:00:05.000Z,d2,50,"
        };
    tableResultSetEqualTest(
        "SELECT t1.time as time1, t1.device as device1, t1.value as value1, \n"
            + "       t2.time as time2, t2.device as device2, t2.value as value2 \n"
            + "FROM \n"
            + "table1 t1 ASOF LEFT JOIN table2 t2\n"
            + "ON\n"
            + "t1.device=t2.device AND t1.time>=t2.time\n"
            + "order by time1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "2020-01-01T00:00:01.000Z,d1,1,2020-01-01T00:00:02.000Z,d1,20,",
          "2020-01-01T00:00:03.000Z,d1,3,2020-01-01T00:00:04.000Z,d2,40,",
          "2020-01-01T00:00:05.000Z,d1,5,null,null,null,",
          "2020-01-01T00:00:08.000Z,d2,8,null,null,null,"
        };
    tableResultSetEqualTest(
        "SELECT t1.time as time1, t1.device as device1, t1.value as value1, \n"
            + "       t2.time as time2, t2.device as device2, t2.value as value2 \n"
            + "FROM \n"
            + "table1 t1 ASOF LEFT JOIN table2 t2\n"
            + "ON\n"
            + "t1.time<t2.time\n"
            + "order by time1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "2020-01-01T00:00:01.000Z,d1,1,2020-01-01T00:00:02.000Z,d1,20,",
          "2020-01-01T00:00:03.000Z,d1,3,null,null,null,",
          "2020-01-01T00:00:05.000Z,d1,5,null,null,null,",
          "2020-01-01T00:00:08.000Z,d2,8,null,null,null,"
        };
    tableResultSetEqualTest(
        "SELECT t1.time as time1, t1.device as device1, t1.value as value1, \n"
            + "       t2.time as time2, t2.device as device2, t2.value as value2 \n"
            + "FROM \n"
            + "table1 t1 ASOF LEFT JOIN table2 t2\n"
            + "ON\n"
            + "t1.device=t2.device AND t1.time<t2.time\n"
            + "order by time1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
