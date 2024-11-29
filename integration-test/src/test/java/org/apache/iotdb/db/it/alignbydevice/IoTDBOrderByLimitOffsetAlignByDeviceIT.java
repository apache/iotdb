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
package org.apache.iotdb.db.it.alignbydevice;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.alignbydevice.IoTDBOrderByWithAlignByDeviceIT.insertData;
import static org.apache.iotdb.db.it.alignbydevice.IoTDBOrderByWithAlignByDeviceIT.insertData2;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBOrderByLimitOffsetAlignByDeviceIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
    insertData2();
    insertData3();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  String[] expectedHeader;
  String[] retArray;

  @Test
  public void singleDeviceTest() {
    expectedHeader = new String[] {"Time,Device,precipitation"};
    retArray = new String[] {"1668960000200,root.weather.London,1667492178318,"};
    resultSetEqualTest(
        "select precipitation from root.weather.London where precipitation>1667492178118 order by time offset 1 limit 1 align by device",
        expectedHeader,
        retArray);

    retArray = new String[] {"1668960000200,root.weather.London,1667492178318,"};
    resultSetEqualTest(
        "select precipitation from root.weather.London where precipitation>1667492178118 order by precipitation offset 1 limit 1 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void orderByCanNotPushLimitTest() {
    // 1. value filter, can not push down LIMIT
    expectedHeader = new String[] {"Time,Device,s1"};
    retArray = new String[] {"3,root.db.d1,111,"};
    resultSetEqualTest(
        "SELECT * FROM root.db.** WHERE s1>40 ORDER BY TIME LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 2. order by expression, can not push down LIMIT
    retArray = new String[] {"3,root.db.d3,333,"};
    resultSetEqualTest(
        "SELECT * FROM root.db.** ORDER BY s1 DESC LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 3. time filter, can push down LIMIT
    retArray = new String[] {"2,root.db.d3,33,", "2,root.db.d2,22,"};
    resultSetEqualTest(
        "SELECT * FROM root.db.** WHERE time>1 and time<3 ORDER BY device DESC LIMIT 2 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 4. both exist OFFSET and LIMIT, can push down LIMIT as OFFSET+LIMIT
    retArray = new String[] {"3,root.db.d2,222,"};
    resultSetEqualTest(
        "SELECT * FROM root.db.** ORDER BY time DESC OFFSET 1 LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void aggregationWithHavingTest() {
    // when aggregation with having, can only use MergeSortNode but not use TopKNode
    String[] expectedHeader = new String[] {"Time,Device,sum(s1)"};
    String[] retArray = new String[] {"3,root.db.d2,222.0,", "3,root.db.d3,333.0,"};
    resultSetEqualTest(
        "select sum(s1) from root.db.** group by ((1,5],1ms) having(sum(s1)>111) order by time limit 2 align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void fillTest() {
    // linear fill can not use TopKNode
    String[] expectedHeader = new String[] {"Time,Device,s1,s2"};
    String[] retArray =
        new String[] {
          "1,root.fill.d1,1,null,",
          "1,root.fill.d2,2,null,",
          "1,root.fill.d3,3,null,",
          "2,root.fill.d1,22,11.0,",
        };
    resultSetEqualTest(
        "select * from root.fill.** order by time fill(linear) limit 4 align by device;",
        expectedHeader,
        retArray);
  }

  private static final String[] SQL_LIST =
      new String[] {
        "CREATE DATABASE root.db;",
        "CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE;",
        "CREATE TIMESERIES root.db.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE;",
        "CREATE TIMESERIES root.db.d3.s1 WITH DATATYPE=INT32, ENCODING=RLE;",
        "INSERT INTO root.db.d1(timestamp,s1) VALUES(1, 1), (2, 11), (3, 111);",
        "INSERT INTO root.db.d2(timestamp,s1) VALUES(1, 2), (2, 22), (3, 222);",
        "INSERT INTO root.db.d3(timestamp,s1) VALUES(1, 3), (2, 33), (3, 333);",
        "CREATE DATABASE root.fill;",
        "CREATE TIMESERIES root.fill.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE;",
        "CREATE TIMESERIES root.fill.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE;",
        "CREATE TIMESERIES root.fill.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE;",
        "CREATE TIMESERIES root.fill.d2.s2 WITH DATATYPE=FLOAT, ENCODING=RLE;",
        "CREATE TIMESERIES root.fill.d3.s1 WITH DATATYPE=INT32, ENCODING=RLE;",
        "CREATE TIMESERIES root.fill.d3.s2 WITH DATATYPE=FLOAT, ENCODING=RLE;",
        "INSERT INTO root.fill.d1(timestamp,s1,s2) VALUES(1, 1, null), (2, null, 11), (3, 111, 111.1);",
        "INSERT INTO root.fill.d2(timestamp,s1,s2) VALUES(1, 2, null), (2, 22, 22.2), (3, 222, null);",
        "INSERT INTO root.fill.d3(timestamp,s1,s2) VALUES(1, 3, null), (2, 33, null), (3, 333, 333.3);",
      };

  protected static void insertData3() {
    try (Connection iotDBConnection = EnvFactory.getEnv().getConnection();
        Statement statement = iotDBConnection.createStatement()) {
      for (String sql : SQL_LIST) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
