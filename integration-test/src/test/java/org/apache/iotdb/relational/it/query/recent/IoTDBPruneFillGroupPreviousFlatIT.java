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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBPruneFillGroupPreviousFlatIT {

  private static final String DATABASE_NAME = "test";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE m1(device_id STRING TAG, s1 INT64 FIELD)",
        "INSERT INTO m1(time, device_id, s1) values(2024-09-24T06:15:46.565+00:00, 'd1', 2)",
        "INSERT INTO m1(time, device_id, s1) values(2024-09-24T07:16:15.297+00:00, 'd1', 10)",
        "INSERT INTO m1(time, device_id, s1) values(2024-09-24T08:16:21.907+00:00, 'd2', 20)",
      };

  private static final String FLAT_PREVIOUS_FILL_SQL =
      "SELECT date_bin_gapfill(1h, time) AS h, device_id, avg(s1) AS v "
          + "FROM m1 "
          + "WHERE time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00 "
          + "GROUP BY 1, 2 "
          + "FILL METHOD PREVIOUS TIME_COLUMN 1 FILL_GROUP 2 "
          + "ORDER BY 1, 2";

  private static final String NESTED_OMIT_DEVICE_SQL =
      "SELECT h AS tm, sum(v) AS sv "
          + "FROM ( "
          + "  SELECT date_bin_gapfill(1h, time) AS h, device_id, avg(s1) AS v "
          + "  FROM m1 "
          + "  WHERE time >= 2024-09-24T04:00:00.000+00:00 AND time < 2024-09-24T12:00:00.00+00:00 "
          + "  GROUP BY 1, 2 "
          + "  FILL METHOD PREVIOUS TIME_COLUMN 1 FILL_GROUP 2 "
          + ") t "
          + "GROUP BY h ORDER BY h";

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void flatPreviousFill() {
    String[] header = new String[] {"h", "device_id", "v"};
    String[] rows =
        new String[] {
          "2024-09-24T04:00:00.000Z,d1,null,",
          "2024-09-24T04:00:00.000Z,d2,null,",
          "2024-09-24T05:00:00.000Z,d1,null,",
          "2024-09-24T05:00:00.000Z,d2,null,",
          "2024-09-24T06:00:00.000Z,d1,2.0,",
          "2024-09-24T06:00:00.000Z,d2,null,",
          "2024-09-24T07:00:00.000Z,d1,10.0,",
          "2024-09-24T07:00:00.000Z,d2,null,",
          "2024-09-24T08:00:00.000Z,d1,10.0,",
          "2024-09-24T08:00:00.000Z,d2,20.0,",
          "2024-09-24T09:00:00.000Z,d1,10.0,",
          "2024-09-24T09:00:00.000Z,d2,20.0,",
          "2024-09-24T10:00:00.000Z,d1,10.0,",
          "2024-09-24T10:00:00.000Z,d2,20.0,",
          "2024-09-24T11:00:00.000Z,d1,10.0,",
          "2024-09-24T11:00:00.000Z,d2,20.0,",
        };
    tableResultSetEqualTest(FLAT_PREVIOUS_FILL_SQL, header, rows, DATABASE_NAME);
  }

  @Test
  public void nestedOmitTag() {
    String[] header = new String[] {"tm", "sv"};
    String[] rows =
        new String[] {
          "2024-09-24T04:00:00.000Z,null,",
          "2024-09-24T05:00:00.000Z,null,",
          "2024-09-24T06:00:00.000Z,2.0,",
          "2024-09-24T07:00:00.000Z,10.0,",
          "2024-09-24T08:00:00.000Z,30.0,",
          "2024-09-24T09:00:00.000Z,30.0,",
          "2024-09-24T10:00:00.000Z,30.0,",
          "2024-09-24T11:00:00.000Z,30.0,",
        };
    tableResultSetEqualTest(NESTED_OMIT_DEVICE_SQL, header, rows, DATABASE_NAME);
  }
}
