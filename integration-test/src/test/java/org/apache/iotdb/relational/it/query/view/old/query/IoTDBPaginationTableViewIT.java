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

package org.apache.iotdb.relational.it.query.view.old.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBPaginationTableViewIT {
  private static final String DATABASE_NAME = "test";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.vehicle",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
        "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
        "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
        "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
        "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
        "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
        "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
        "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
        "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
        "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
        "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
        "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
        "CREATE DATABASE root.db",
        "CREATE TIMESERIES root.db.d1.s1 INT32",
        "insert into root.db.d1(timestamp,s1) values(0, 0)",
        "insert into root.db.d1(timestamp,s1) values(1, 1)",
        "insert into root.db.d1(timestamp,s1) values(2, 2)",
        "insert into root.db.d1(timestamp,s1) values(3, 3)",
        "insert into root.db.d1(timestamp,s1) values(4, 4)",
        "flush",
        "insert into root.db.d1(timestamp,s1) values(5, 5)",
        "insert into root.db.d1(timestamp,s1) values(6, 6)",
        "insert into root.db.d1(timestamp,s1) values(7, 7)",
        "insert into root.db.d1(timestamp,s1) values(8, 8)",
        "insert into root.db.d1(timestamp,s1) values(9, 9)",
        "flush",
        "insert into root.db.d1(timestamp,s1) values(10, 10)",
        "insert into root.db.d1(timestamp,s1) values(11, 11)",
        "insert into root.db.d1(timestamp,s1) values(12, 12)",
        "insert into root.db.d1(timestamp,s1) values(13, 13)",
        "insert into root.db.d1(timestamp,s1) values(14, 14)"
      };
  private static final String[] CREATE_TABLE_VIEW_SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE VIEW vehicle(device STRING TAG, s0 INT32 FIELD, s1 INT64 FIELD, s2 FLOAT FIELD) as root.vehicle.**",
        "CREATE VIEW db(device STRING TAG, s1 INT32 FIELD) as root.db.**",
      };

  @BeforeClass
  public static void setUp() throws InterruptedException {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
    prepareTableData(CREATE_TABLE_VIEW_SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void rawDataQueryTest() {
    List<String> querySQLs =
        Arrays.asList(
            "SELECT time,s1 FROM vehicle WHERE time<200 limit 3",
            "SELECT time,s0 FROM vehicle WHERE s1 > 190 limit 3",
            "SELECT time,s1,s2 FROM vehicle WHERE s1 > 190 or s2 < 10.0 offset 2 limit 3",
            "SELECT time,s2 FROM vehicle WHERE s1 > 190 or s2 < 10.0 offset 1 limit 3");
    List<List<String>> expectHeaders =
        Arrays.asList(
            Arrays.asList("time", "s1"),
            Arrays.asList("time", "s0"),
            Arrays.asList("time", "s1", "s2"),
            Arrays.asList("time", "s2"));
    List<String[]> retArrays =
        Arrays.asList(
            new String[] {
              defaultFormatDataTime(1) + ",1101,",
              defaultFormatDataTime(2) + ",40000,",
              defaultFormatDataTime(50) + ",50000,",
            },
            new String[] {
              defaultFormatDataTime(1) + ",101,",
              defaultFormatDataTime(2) + ",10000,",
              defaultFormatDataTime(50) + ",10000,"
            },
            new String[] {
              defaultFormatDataTime(3) + ",null,3.33,",
              defaultFormatDataTime(4) + ",null,4.44,",
              defaultFormatDataTime(50) + ",50000,null,"
            },
            new String[] {
              defaultFormatDataTime(2) + ",2.22,",
              defaultFormatDataTime(3) + ",3.33,",
              defaultFormatDataTime(4) + ",4.44,",
            });

    for (int i = 0; i < querySQLs.size(); i++) {
      tableResultSetEqualTest(
          querySQLs.get(i),
          expectHeaders.get(i).toArray(new String[0]),
          retArrays.get(i),
          DATABASE_NAME);
    }
  }

  @Test
  public void limitOffsetPushDownTest() {
    String[] expectedHeader = new String[] {"s1"};
    String[] retArray =
        new String[] {
          "3,",
        };
    tableResultSetEqualTest(
        "select s1 from db where time > 1 offset 1 limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "5,",
        };
    tableResultSetEqualTest(
        "select s1 from db where time > 1 offset 3 limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "7,",
        };
    tableResultSetEqualTest(
        "select s1 from db where time > 1 offset 5 limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "12,",
        };
    tableResultSetEqualTest(
        "select s1 from db where time > 1 offset 10 limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "1,",
        };
    tableResultSetEqualTest(
        "select s1 from db order by time limit 1 offset 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select s1 from db order by device limit 1 offset 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
