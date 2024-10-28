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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBGapFillTableIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(city STRING ID, device_id STRING ID, s1 DOUBLE MEASUREMENT, s2 INT64 MEASUREMENT)",
        "INSERT INTO table1(time,city,device_id,s2) values(2024-09-24T06:15:46.565+00:00, 'shanghai', 'd1', 2)",
        "INSERT INTO table1(time,city,device_id,s1) values(2024-09-24T07:16:15.297+00:00, 'shanghai', 'd1', 27.2)",
        "INSERT INTO table1(time,city,device_id,s1) values(2024-09-24T08:16:21.907+00:00, 'shanghai', 'd1', 27.3)",
        "INSERT INTO table1(time,city,device_id,s1) values(2024-09-24T11:16:28.821+00:00, 'shanghai', 'd1', 29.3)",
        "INSERT INTO table1(time,city,device_id,s1) values(2024-09-24T04:14:40.545+00:00, 'beijing', 'd2', 25.1)",
        "INSERT INTO table1(time,city,device_id,s1) values(2024-09-24T11:17:15.297+00:00, 'beijing', 'd2', 28.2)",
        "INSERT INTO table1(time,city,device_id,s1) values(2024-09-24T08:15:21.907+00:00, 'shanghai', 'd3', 22.3)",
        "INSERT INTO table1(time,city,device_id,s1) values(2024-09-24T08:15:28.821+00:00, 'shanghai', 'd3', 29.3)",
      };

  private static final String MULTI_GAFILL_ERROR_MSG =
      TSStatusCode.SEMANTIC_ERROR.getStatusCode() + ": multiple date_bin_gapfill calls not allowed";
  private static final String TIME_RANGE_CANNOT_INFER_ERROR_MSG =
      TSStatusCode.SEMANTIC_ERROR.getStatusCode()
          + ": could not infer startTime or endTime from WHERE clause";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void normalGapFillTest() {

    // case 1: avg_s1 of one device without having
    String[] expectedHeader = new String[] {"hour_time", "avg_s1"};
    String[] retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,null,",
          "2024-09-24T05:00:00.000Z,null,",
          "2024-09-24T06:00:00.000Z,null,",
          "2024-09-24T07:00:00.000Z,27.2,",
          "2024-09-24T08:00:00.000Z,27.3,",
          "2024-09-24T09:00:00.000Z,null,",
          "2024-09-24T10:00:00.000Z,null,",
          "2024-09-24T11:00:00.000Z,29.3,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1,city,device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 2: avg_s1 of one device with having
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1 HAVING avg(s1) IS NOT NULL",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1,city,device_id HAVING avg(s1) IS NOT NULL",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 3: avg_s1 of each device
    expectedHeader = new String[] {"hour_time", "city", "device_id", "avg_s1"};
    retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T05:00:00.000Z,beijing,d2,null,",
          "2024-09-24T06:00:00.000Z,beijing,d2,null,",
          "2024-09-24T07:00:00.000Z,beijing,d2,null,",
          "2024-09-24T08:00:00.000Z,beijing,d2,null,",
          "2024-09-24T09:00:00.000Z,beijing,d2,null,",
          "2024-09-24T10:00:00.000Z,beijing,d2,null,",
          "2024-09-24T11:00:00.000Z,beijing,d2,28.2,",
          "2024-09-24T04:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T05:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T06:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T07:00:00.000Z,shanghai,d1,27.2,",
          "2024-09-24T08:00:00.000Z,shanghai,d1,27.3,",
          "2024-09-24T09:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T10:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T11:00:00.000Z,shanghai,d1,29.3,",
          "2024-09-24T04:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T05:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T06:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T07:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T08:00:00.000Z,shanghai,d3,25.8,",
          "2024-09-24T09:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T10:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T11:00:00.000Z,shanghai,d3,null,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, device_id, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2,device_id order by city,3,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, device_id, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY date_bin_gapfill(1h, time),city,device_id order by city,device_id,date_bin_gapfill(1h, time)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, device_id, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2,3 order by 2,3,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 4: avg_s1 of all devices
    expectedHeader = new String[] {"hour_time", "avg_s1"};
    retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,25.1,",
          "2024-09-24T05:00:00.000Z,null,",
          "2024-09-24T06:00:00.000Z,null,",
          "2024-09-24T07:00:00.000Z,27.2,",
          "2024-09-24T08:00:00.000Z,26.3,",
          "2024-09-24T09:00:00.000Z,null,",
          "2024-09-24T10:00:00.000Z,null,",
          "2024-09-24T11:00:00.000Z,28.75,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1 order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY date_bin_gapfill(1h, time) order by date_bin_gapfill(1h, time)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1 order by date_bin_gapfill(1h, time)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY date_bin_gapfill(1h, time) order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 5: avg_s1 of all cities
    expectedHeader = new String[] {"hour_time", "city", "avg_s1"};
    retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,beijing,25.1,",
          "2024-09-24T05:00:00.000Z,beijing,null,",
          "2024-09-24T06:00:00.000Z,beijing,null,",
          "2024-09-24T07:00:00.000Z,beijing,null,",
          "2024-09-24T08:00:00.000Z,beijing,null,",
          "2024-09-24T09:00:00.000Z,beijing,null,",
          "2024-09-24T10:00:00.000Z,beijing,null,",
          "2024-09-24T11:00:00.000Z,beijing,28.2,",
          "2024-09-24T04:00:00.000Z,shanghai,null,",
          "2024-09-24T05:00:00.000Z,shanghai,null,",
          "2024-09-24T06:00:00.000Z,shanghai,null,",
          "2024-09-24T07:00:00.000Z,shanghai,27.2,",
          "2024-09-24T08:00:00.000Z,shanghai,26.3,",
          "2024-09-24T09:00:00.000Z,shanghai,null,",
          "2024-09-24T10:00:00.000Z,shanghai,null,",
          "2024-09-24T11:00:00.000Z,shanghai,29.3,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2 order by 2,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 6: avg_s1 of all device_ids
    expectedHeader = new String[] {"hour_time", "device_id", "avg_s1"};
    retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,d1,null,",
          "2024-09-24T05:00:00.000Z,d1,null,",
          "2024-09-24T06:00:00.000Z,d1,null,",
          "2024-09-24T07:00:00.000Z,d1,27.2,",
          "2024-09-24T08:00:00.000Z,d1,27.3,",
          "2024-09-24T09:00:00.000Z,d1,null,",
          "2024-09-24T10:00:00.000Z,d1,null,",
          "2024-09-24T11:00:00.000Z,d1,29.3,",
          "2024-09-24T04:00:00.000Z,d2,25.1,",
          "2024-09-24T05:00:00.000Z,d2,null,",
          "2024-09-24T06:00:00.000Z,d2,null,",
          "2024-09-24T07:00:00.000Z,d2,null,",
          "2024-09-24T08:00:00.000Z,d2,null,",
          "2024-09-24T09:00:00.000Z,d2,null,",
          "2024-09-24T10:00:00.000Z,d2,null,",
          "2024-09-24T11:00:00.000Z,d2,28.2,",
          "2024-09-24T04:00:00.000Z,d3,null,",
          "2024-09-24T05:00:00.000Z,d3,null,",
          "2024-09-24T06:00:00.000Z,d3,null,",
          "2024-09-24T07:00:00.000Z,d3,null,",
          "2024-09-24T08:00:00.000Z,d3,25.8,",
          "2024-09-24T09:00:00.000Z,d3,null,",
          "2024-09-24T10:00:00.000Z,d3,null,",
          "2024-09-24T11:00:00.000Z,d3,null,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, device_id, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2 order by 2,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 7: no data after where
    expectedHeader = new String[] {"hour_time", "avg_s1"};
    retArray = new String[] {};
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T00:00:00.000+00:00 AND time < 2024-09-24T06:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void gapFillWithFillClauseTest() {

    // case 1: avg_s1 of one device without having
    String[] expectedHeader = new String[] {"hour_time", "avg_s1"};
    String[] retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,null,",
          "2024-09-24T05:00:00.000Z,null,",
          "2024-09-24T06:00:00.000Z,null,",
          "2024-09-24T07:00:00.000Z,27.2,",
          "2024-09-24T08:00:00.000Z,27.3,",
          "2024-09-24T09:00:00.000Z,27.3,",
          "2024-09-24T10:00:00.000Z,27.3,",
          "2024-09-24T11:00:00.000Z,29.3,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1 FILL METHOD PREVIOUS",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1,city,device_id FILL METHOD PREVIOUS",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 2: avg_s1 of one device with having
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1 HAVING avg(s1) IS NOT NULL FILL METHOD PREVIOUS",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1,device_id,city HAVING avg(s1) IS NOT NULL FILL METHOD PREVIOUS",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 3: avg_s1 of each device
    expectedHeader = new String[] {"hour_time", "city", "device_id", "avg_s1"};
    retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T05:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T06:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T07:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T08:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T09:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T10:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T11:00:00.000Z,beijing,d2,28.2,",
          "2024-09-24T04:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T05:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T06:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T07:00:00.000Z,shanghai,d1,27.2,",
          "2024-09-24T08:00:00.000Z,shanghai,d1,27.3,",
          "2024-09-24T09:00:00.000Z,shanghai,d1,27.3,",
          "2024-09-24T10:00:00.000Z,shanghai,d1,27.3,",
          "2024-09-24T11:00:00.000Z,shanghai,d1,29.3,",
          "2024-09-24T04:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T05:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T06:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T07:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T08:00:00.000Z,shanghai,d3,25.8,",
          "2024-09-24T09:00:00.000Z,shanghai,d3,25.8,",
          "2024-09-24T10:00:00.000Z,shanghai,d3,25.8,",
          "2024-09-24T11:00:00.000Z,shanghai,d3,25.8,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, device_id, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2,device_id FILL METHOD PREVIOUS FILL_GROUP 2,3 order by city,3,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, device_id, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY date_bin_gapfill(1h, time),city,device_id FILL METHOD PREVIOUS FILL_GROUP 2,3 order by city,device_id,date_bin_gapfill(1h, time)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, device_id, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2,3 FILL METHOD PREVIOUS FILL_GROUP 2,3 order by 2,3,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T05:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T06:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T07:00:00.000Z,shanghai,d1,27.2,",
          "2024-09-24T08:00:00.000Z,shanghai,d1,27.3,",
          "2024-09-24T09:00:00.000Z,shanghai,d1,27.3,",
          "2024-09-24T10:00:00.000Z,shanghai,d1,null,",
          "2024-09-24T11:00:00.000Z,shanghai,d1,29.3,",
          "2024-09-24T04:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T05:00:00.000Z,beijing,d2,25.1,",
          "2024-09-24T06:00:00.000Z,beijing,d2,null,",
          "2024-09-24T07:00:00.000Z,beijing,d2,null,",
          "2024-09-24T08:00:00.000Z,beijing,d2,null,",
          "2024-09-24T09:00:00.000Z,beijing,d2,null,",
          "2024-09-24T10:00:00.000Z,beijing,d2,null,",
          "2024-09-24T11:00:00.000Z,beijing,d2,28.2,",
          "2024-09-24T04:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T05:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T06:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T07:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T08:00:00.000Z,shanghai,d3,25.8,",
          "2024-09-24T09:00:00.000Z,shanghai,d3,25.8,",
          "2024-09-24T10:00:00.000Z,shanghai,d3,null,",
          "2024-09-24T11:00:00.000Z,shanghai,d3,null,",
        };
    // with time bound
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, device_id, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2,3 FILL METHOD PREVIOUS TIME_BOUND 1h FILL_GROUP 2,3 order by 3,2,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 4: avg_s1 of all devices
    expectedHeader = new String[] {"hour_time", "avg_s1"};
    retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,25.1,",
          "2024-09-24T05:00:00.000Z,25.1,",
          "2024-09-24T06:00:00.000Z,25.1,",
          "2024-09-24T07:00:00.000Z,27.2,",
          "2024-09-24T08:00:00.000Z,26.3,",
          "2024-09-24T09:00:00.000Z,26.3,",
          "2024-09-24T10:00:00.000Z,26.3,",
          "2024-09-24T11:00:00.000Z,28.75,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1 FILL METHOD PREVIOUS order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY date_bin_gapfill(1h, time) FILL METHOD PREVIOUS order by date_bin_gapfill(1h, time)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1 FILL METHOD PREVIOUS order by date_bin_gapfill(1h, time)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY date_bin_gapfill(1h, time) FILL METHOD PREVIOUS order by 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 5: avg_s1 of all cities
    expectedHeader = new String[] {"hour_time", "city", "avg_s1"};
    retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,beijing,25.1,",
          "2024-09-24T05:00:00.000Z,beijing,25.1,",
          "2024-09-24T06:00:00.000Z,beijing,25.1,",
          "2024-09-24T07:00:00.000Z,beijing,25.1,",
          "2024-09-24T08:00:00.000Z,beijing,25.1,",
          "2024-09-24T09:00:00.000Z,beijing,25.1,",
          "2024-09-24T10:00:00.000Z,beijing,25.1,",
          "2024-09-24T11:00:00.000Z,beijing,28.2,",
          "2024-09-24T04:00:00.000Z,shanghai,null,",
          "2024-09-24T05:00:00.000Z,shanghai,null,",
          "2024-09-24T06:00:00.000Z,shanghai,null,",
          "2024-09-24T07:00:00.000Z,shanghai,27.2,",
          "2024-09-24T08:00:00.000Z,shanghai,26.3,",
          "2024-09-24T09:00:00.000Z,shanghai,26.3,",
          "2024-09-24T10:00:00.000Z,shanghai,26.3,",
          "2024-09-24T11:00:00.000Z,shanghai,29.3,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2 FILL METHOD PREVIOUS TIME_COLUMN 1 FILL_GROUP 2 order by 2,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, city, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2 FILL METHOD PREVIOUS FILL_GROUP 2 order by 2,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 6: avg_s1 of all device_ids
    expectedHeader = new String[] {"hour_time", "device_id", "avg_s1"};
    retArray =
        new String[] {
          "2024-09-24T04:00:00.000Z,d1,null,",
          "2024-09-24T05:00:00.000Z,d1,null,",
          "2024-09-24T06:00:00.000Z,d1,null,",
          "2024-09-24T07:00:00.000Z,d1,27.2,",
          "2024-09-24T08:00:00.000Z,d1,27.3,",
          "2024-09-24T09:00:00.000Z,d1,27.3,",
          "2024-09-24T10:00:00.000Z,d1,27.3,",
          "2024-09-24T11:00:00.000Z,d1,29.3,",
          "2024-09-24T04:00:00.000Z,d2,25.1,",
          "2024-09-24T05:00:00.000Z,d2,25.1,",
          "2024-09-24T06:00:00.000Z,d2,25.1,",
          "2024-09-24T07:00:00.000Z,d2,25.1,",
          "2024-09-24T08:00:00.000Z,d2,25.1,",
          "2024-09-24T09:00:00.000Z,d2,25.1,",
          "2024-09-24T10:00:00.000Z,d2,25.1,",
          "2024-09-24T11:00:00.000Z,d2,28.2,",
          "2024-09-24T04:00:00.000Z,d3,null,",
          "2024-09-24T05:00:00.000Z,d3,null,",
          "2024-09-24T06:00:00.000Z,d3,null,",
          "2024-09-24T07:00:00.000Z,d3,null,",
          "2024-09-24T08:00:00.000Z,d3,25.8,",
          "2024-09-24T09:00:00.000Z,d3,25.8,",
          "2024-09-24T10:00:00.000Z,d3,25.8,",
          "2024-09-24T11:00:00.000Z,d3,25.8,",
        };
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, device_id, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00) GROUP BY 1,2 FILL METHOD PREVIOUS FILL_GROUP 2 order by 2,1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 7: no data after where
    expectedHeader = new String[] {"hour_time", "avg_s1"};
    retArray = new String[] {};
    tableResultSetEqualTest(
        "select date_bin_gapfill(1h, time) as hour_time, avg(s1) as avg_s1 from table1 where (time >= 2024-09-24T00:00:00.000+00:00 AND time < 2024-09-24T06:00:00.00+00:00) AND device_id = 'd1' GROUP BY 1 FILL METHOD PREVIOUS",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void abNormalGapFillTest() {

    // case 1: multiple date_bin_gapfill in group by clause
    tableAssertTestFail(
        "select date_bin_gapfill(1s, time), city, device_id, avg(s1)+avg(s2) from table1 where device_id = 'd1' group by date_bin_gapfill(1s, time), date_bin_gapfill(2s, time), 2,3 having avg(s2) is not null FILL METHOD PREVIOUS FILL_GROUP 2,3 order by 2,3,1",
        MULTI_GAFILL_ERROR_MSG,
        DATABASE_NAME);

    // case 2: no time filter
    tableAssertTestFail(
        "select date_bin_gapfill(1s, time), city, device_id, avg(s1)+avg(s2) from table1 where device_id = 'd1' group by 1,2,3 having avg(s2) is not null FILL METHOD PREVIOUS FILL_GROUP 2,3 order by 2,3,1",
        TIME_RANGE_CANNOT_INFER_ERROR_MSG,
        DATABASE_NAME);

    // case 3: with or
    tableAssertTestFail(
        "select date_bin_gapfill(1s, time), city, device_id, avg(s1)+avg(s2) from table1 where (time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.000+00:00) OR device_id = 'd1' group by 1,2,3 having avg(s2) is not null FILL METHOD PREVIOUS FILL_GROUP 2,3 order by 2,3,1",
        TIME_RANGE_CANNOT_INFER_ERROR_MSG,
        DATABASE_NAME);

    // case 4: time filter is not between
    tableAssertTestFail(
        "select date_bin_gapfill(1s, time), city, device_id, avg(s1)+avg(s2) from table1 where (time not between 2024-09-24T04:00:00.000+00:00 AND 2024-09-24T12:00:00.000+00:00) AND (device_id NOT IN ('d3')) group by 1,2,3 having avg(s2) is not null FILL METHOD PREVIOUS FILL_GROUP 2,3 order by 2,3,1",
        TIME_RANGE_CANNOT_INFER_ERROR_MSG,
        DATABASE_NAME);

    // case 5: only >
    tableAssertTestFail(
        "select date_bin_gapfill(1s, time), city, device_id, avg(s1)+avg(s2) from table1 where time > 2024-09-24T04:00:00.000+00:00 AND device_id = 'd1' group by 1,2,3 having avg(s2) is not null FILL METHOD PREVIOUS FILL_GROUP 2,3 order by 2,3,1",
        TIME_RANGE_CANNOT_INFER_ERROR_MSG,
        DATABASE_NAME);

    // case 6: only >=
    tableAssertTestFail(
        "select date_bin_gapfill(1s, time), city, device_id, avg(s1)+avg(s2) from table1 where time >= 2024-09-24T04:00:00.000+00:00 group by 1,2,3 having avg(s2) is not null FILL METHOD PREVIOUS FILL_GROUP 2,3 order by 2,3,1",
        TIME_RANGE_CANNOT_INFER_ERROR_MSG,
        DATABASE_NAME);

    // case 7: only <
    tableAssertTestFail(
        "select date_bin_gapfill(1s, time), city, device_id, avg(s1)+avg(s2) from table1 where time < 2024-09-24T12:00:00.000+00:00 AND device_id = 'd1' group by 1,2,3 having avg(s2) is not null FILL METHOD PREVIOUS FILL_GROUP 2,3 order by 2,3,1",
        TIME_RANGE_CANNOT_INFER_ERROR_MSG,
        DATABASE_NAME);

    // case 8: only <=
    tableAssertTestFail(
        "select date_bin_gapfill(1s, time), city, device_id, avg(s1)+avg(s2) from table1 where time <= 2024-09-24T12:00:00.000+00:00 group by 1,2,3 having avg(s2) is not null FILL METHOD PREVIOUS FILL_GROUP 2,3 order by 2,3,1",
        TIME_RANGE_CANNOT_INFER_ERROR_MSG,
        DATABASE_NAME);
  }
}
