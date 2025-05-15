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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

/**
 * In this Class, we construct the scenario using DistinctAccumulator for
 * AggregationFunctionDistinct. Other cases are covered by {@link IoTDBTableViewAggregationIT}.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableViewAggregationFunctionDistinctIT {

  private static final String TREE_DB_NAME = "root.test";
  private static final String DATABASE_NAME = "test";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + TREE_DB_NAME,
        "CREATE ALIGNED TIMESERIES root.test.table1.d01(s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT, s7 STRING, s8 BLOB, s9 TIMESTAMP, s10 DATE)",
        "INSERT INTO root.test.table1.d01(time,s1,s3,s6,s8,s9) aligned values (2024-09-24T06:15:30.000+00:00,30,30.0,'shanghai_huangpu_red_A_d01_30', X'cafebabe30',2024-09-24T06:15:30.000+00:00)",
        "INSERT INTO root.test.table1.d01(time,s2,s3,s4,s6,s7,s9,s10) aligned values (2024-09-24T06:15:35.000+00:00,35000,35.0,35.0,'shanghai_huangpu_red_A_d01_35','shanghai_huangpu_red_A_d01_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d01(time,s1,s3,s5,s7,s9) aligned values (2024-09-24T06:15:40.000+00:00,40,40.0,true,'shanghai_huangpu_red_A_d01_40',2024-09-24T06:15:40.000+00:00)",
        "INSERT INTO root.test.table1.d01(time,s2,s5,s9,s10) aligned values (2024-09-24T06:15:50.000+00:00,50000,false,2024-09-24T06:15:50.000+00:00,'2024-09-24')",
        "INSERT INTO root.test.table1.d01(time,s1,s2,s3,s4,s6,s7,s9,s10) aligned values (2024-09-24T06:15:35.000+00:00,30,35000,35.0,35.0,'shanghai_huangpu_red_A_d01_35','shanghai_huangpu_red_A_d01_35',2024-09-24T06:15:35.000+00:00,'2024-09-24')",
        "FLUSH",
      };
  private static final String[] createTableViewSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE VIEW table1(device_id STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD, s3 FLOAT FIELD, s4 DOUBLE FIELD, s5 BOOLEAN FIELD, s6 TEXT FIELD, s7 STRING FIELD, s8 BLOB FIELD, s9 TIMESTAMP FIELD, s10 DATE FIELD) as root.test.table1.**",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(createSqls);
    prepareTableData(createTableViewSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void countDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10"
        };
    String[] retArray = new String[] {"d01,2,2,3,1,2,2,2,1,4,1,"};

    tableResultSetEqualTest(
        "select device_id, count(distinct s1), count(distinct s2), count(distinct s3), count(distinct s4), count(distinct s5), count(distinct s6), count(distinct s7), count(distinct s8), count(distinct s9), count(distinct s10) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void countIfDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10"
        };
    String[] retArray = new String[] {"d01,0,1,1,1,1,1,1,1,1,1,"};
    tableResultSetEqualTest(
        "select device_id, count_if(distinct s1 < 0), count_if(distinct s2 is not null), count_if(distinct s3 is not null), count_if(distinct s4 is not null), count_if(distinct s5 is not null), count_if(distinct s6 is not null), count_if(distinct s7 is not null), count_if(distinct s8 is not null), count_if(distinct s9 is not null), count_if(distinct s10 is not null) "
            + "from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void avgDistinctTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2", "_col3", "_col4"};
    String[] retArray =
        new String[] {
          "d01,35.0,42500.0,35.0,35.0,",
        };
    tableResultSetEqualTest(
        "select device_id, avg(distinct s1), avg(distinct s2), avg(distinct s3), avg(distinct s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void sumDistinctTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2", "_col3", "_col4"};
    String[] retArray = new String[] {"d01,70.0,85000.0,105.0,35.0,"};
    tableResultSetEqualTest(
        "select device_id, sum(distinct s1), sum(distinct s2), sum(distinct s3), sum(distinct s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void minDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11"
        };
    String[] retArray =
        new String[] {
          "d01,d01,30,35000,30.0,35.0,false,shanghai_huangpu_red_A_d01_30,shanghai_huangpu_red_A_d01_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,"
        };
    tableResultSetEqualTest(
        "select device_id, min(distinct device_id), min(distinct s1), min(distinct s2), min(distinct s3), min(distinct s4), min(distinct s5), min(distinct s6), min(distinct s7), min(distinct s8), min(distinct s9), min(distinct s10)  from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void minByDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8"
        };
    String[] retArray =
        new String[] {
          "d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,2024-09-24T06:15:50.000Z,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,",
        };

    tableResultSetEqualTest(
        "select device_id, min_by(distinct time, s1), min_by(distinct time, s2), min_by(distinct time, s3), min_by(distinct time, s4), min_by(distinct time, s5), min_by(distinct time, s6), min_by(distinct time, s9), min_by(distinct time, s10) "
            + "from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void maxDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10",
          "_col11"
        };
    String[] retArray =
        new String[] {
          "d01,d01,40,50000,40.0,35.0,true,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_40,0xcafebabe30,2024-09-24T06:15:50.000Z,2024-09-24,"
        };
    tableResultSetEqualTest(
        "select device_id,max(distinct device_id), max(distinct s1), max(distinct s2), max(distinct s3), max(distinct s4), max(distinct s5), max(distinct s6), max(distinct s7), max(distinct s8), max(distinct s9), max(distinct s10) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void maxByDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8"
        };
    String[] retArray =
        new String[] {
          "d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:50.000Z,2024-09-24T06:15:40.000Z,2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,2024-09-24T06:15:35.000Z,2024-09-24T06:15:50.000Z,2024-09-24T06:15:35.000Z,",
        };

    tableResultSetEqualTest(
        "select device_id, max_by(distinct time, s1), max_by(distinct time, s2), max_by(distinct time, s3), max_by(distinct time, s4), max_by(distinct time, s5), max_by(distinct time, s6), max_by(distinct time, s9), max_by(distinct time, s10) "
            + "from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void firstDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10"
        };
    String[] retArray =
        new String[] {
          "d01,30,35000,30.0,35.0,true,shanghai_huangpu_red_A_d01_30,shanghai_huangpu_red_A_d01_35,0xcafebabe30,2024-09-24T06:15:30.000Z,2024-09-24,"
        };
    tableResultSetEqualTest(
        "select device_id, first(distinct s1), first(distinct s2), first(distinct s3), first(distinct s4), first(distinct s5), first(distinct s6), first(distinct s7), first(distinct s8), first(distinct s9), first(distinct s10) "
            + "from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void firstByDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10"
        };
    String[] retArray =
        new String[] {
          "d01,2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,2024-09-24T06:15:30.000Z,2024-09-24T06:15:30.000Z,2024-09-24T06:15:35.000Z,"
        };
    tableResultSetEqualTest(
        "select device_id, first_by(distinct time, s1), first_by(distinct time, s2), first_by(distinct time, s3), first_by(distinct time, s4), first_by(distinct time, s5), first_by(distinct time, s6), first_by(distinct time, s7), first_by(distinct time, s8), first_by(distinct time, s9), first_by(distinct time, s10) "
            + "from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void lastDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10"
        };
    String[] retArray =
        new String[] {
          "d01,40,50000,40.0,35.0,false,shanghai_huangpu_red_A_d01_35,shanghai_huangpu_red_A_d01_40,0xcafebabe30,2024-09-24T06:15:50.000Z,2024-09-24,"
        };
    tableResultSetEqualTest(
        "select device_id, last(distinct s1), last(distinct s2), last(distinct s3), last(distinct s4), last(distinct s5), last(distinct s6), last(distinct s7), last(distinct s8), last(distinct s9), last(distinct s10) "
            + "from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void lastByDistinctTest() {
    String[] expectedHeader =
        new String[] {
          "device_id",
          "_col1",
          "_col2",
          "_col3",
          "_col4",
          "_col5",
          "_col6",
          "_col7",
          "_col8",
          "_col9",
          "_col10"
        };
    String[] retArray =
        new String[] {
          "d01,2024-09-24T06:15:40.000Z,2024-09-24T06:15:50.000Z,2024-09-24T06:15:40.000Z,2024-09-24T06:15:35.000Z,2024-09-24T06:15:50.000Z,2024-09-24T06:15:35.000Z,2024-09-24T06:15:40.000Z,2024-09-24T06:15:30.000Z,2024-09-24T06:15:50.000Z,2024-09-24T06:15:35.000Z,"
        };
    tableResultSetEqualTest(
        "select device_id, last_by(distinct time, s1), last_by(distinct time, s2), last_by(distinct time, s3), last_by(distinct time, s4), last_by(distinct time, s5), last_by(distinct time, s6), last_by(distinct time, s7), last_by(distinct time, s8), last_by(distinct time, s9), first_by(distinct time, s10) "
            + "from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void extremeDistinctTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2", "_col3", "_col4"};
    String[] retArray = new String[] {"d01,40,50000,40.0,35.0,"};
    tableResultSetEqualTest(
        "select device_id, extreme(distinct s1), extreme(distinct s2), extreme(distinct s3), extreme(distinct s4) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void varianceDistinctTest() {
    String[] expectedHeader = new String[] {"device_id", "_col1", "_col2", "_col3", "_col4"};
    String[] retArray = new String[] {"d01,25.0,5.625E7,16.7,0.0,"};
    tableResultSetEqualTest(
        "select device_id, round(VAR_POP(distinct s1),1), round(VAR_POP(distinct s2),1), round(VAR_POP(distinct s3),1), round(VAR_POP(distinct s4),1) from table1 group by 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
