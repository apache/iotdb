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

package org.apache.iotdb.relational.it.query.old.alignbydevice;

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
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBOrderByLimitOffsetAlignByDeviceTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData3();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void orderByCanNotPushLimitTest() {
    // 1. value filter, can not push down LIMIT
    String[] expectedHeader = new String[] {"time", "device_id", "s1"};
    String[] retArray = new String[] {"1970-01-01T00:00:00.003Z,d1,111,"};
    tableResultSetEqualTest(
        "SELECT * FROM table1 WHERE s1>40 ORDER BY Time, device_id LIMIT 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 2. order by expression, can not push down LIMIT
    retArray = new String[] {"1970-01-01T00:00:00.003Z,d3,333,"};
    tableResultSetEqualTest(
        "SELECT * FROM table1 ORDER BY s1 DESC LIMIT 1", expectedHeader, retArray, DATABASE_NAME);

    // 3. time filter, can push down LIMIT
    retArray = new String[] {"1970-01-01T00:00:00.002Z,d3,33,", "1970-01-01T00:00:00.002Z,d2,22,"};
    tableResultSetEqualTest(
        "SELECT * FROM table1 WHERE time>1 and time<3 ORDER BY device_id DESC,time LIMIT 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 4. both exist OFFSET and LIMIT, can push down LIMIT as OFFSET+LIMIT
    retArray = new String[] {"1970-01-01T00:00:00.003Z,d2,222,"};
    tableResultSetEqualTest(
        "SELECT * FROM table1 ORDER BY Time DESC, device_id OFFSET 1 LIMIT 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  //
  //  @Test
  //  public void aggregationWithHavingTest() {
  //    // when aggregation with having, can only use MergeSortNode but not use TopKNode
  //    String[] expectedHeader = new String[] {"Time,Device,sum(s1)"};
  //    String[] retArray = new String[] {"3,root.db.d2,222.0,", "3,root.db.d3,333.0,"};
  //    resultSetEqualTest(
  //        "select sum(s1) from root.db.** group by ((1,5],1ms) having(sum(s1)>111) order by time
  // limit 2 align by device",
  //        expectedHeader,
  //        retArray);
  //  }
  //
  //  @Test
  //  public void fillTest() {
  //    // linear fill can not use TopKNode
  //    String[] expectedHeader = new String[] {"Time,Device,s1,s2"};
  //    String[] retArray =
  //        new String[] {
  //          "1,root.fill.d1,1,null,",
  //          "1,root.fill.d2,2,null,",
  //          "1,root.fill.d3,3,null,",
  //          "2,root.fill.d1,22,11.0,",
  //        };
  //    resultSetEqualTest(
  //        "select * from root.fill.** order by time fill(linear) limit 4 align by device",
  //        expectedHeader,
  //        retArray);
  //  }

  private static final String DATABASE_NAME = "db";

  private static final String DATABASE_FILL_NAME = "fill";

  private static final String[] SQL_LIST =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table table1(device_id STRING ID, s1 INT32 MEASUREMENT)",
        "INSERT INTO table1(Time, device_id, s1) VALUES(1, 'd1', 1), (2, 'd1', 11), (3, 'd1', 111)",
        "INSERT INTO table1(Time, device_id, s1) VALUES(1, 'd2', 2), (2, 'd2', 22), (3, 'd2', 222)",
        "INSERT INTO table1(Time, device_id, s1) VALUES(1, 'd3', 3), (2, 'd3', 33), (3, 'd3', 333)",
        "CREATE DATABASE " + DATABASE_FILL_NAME,
        "USE " + DATABASE_FILL_NAME,
        "create table table1(device_id STRING ID, s1 INT32 MEASUREMENT, s2 FLOAT MEASUREMENT)",
        "INSERT INTO table1(Time,device_id,s1,s2) VALUES(1, 'd1', 1, null), (2, 'd1', null, 11), (3, 'd1', 111, 111.1)",
        "INSERT INTO table1(Time,device_id,s1,s2) VALUES(1, 'd2', 2, null), (2, 'd2', 22, 22.2), (3, 'd2', 222, null)",
        "INSERT INTO table1(Time,device_id,s1,s2) VALUES(1, 'd3', 3, null), (2, 'd3', 33, null), (3, 'd3', 333, 333.3)",
      };

  protected static void insertData3() {
    try (Connection iotDBConnection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = iotDBConnection.createStatement()) {
      for (String sql : SQL_LIST) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
