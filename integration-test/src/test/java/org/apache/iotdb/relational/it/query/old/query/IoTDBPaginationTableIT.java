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

package org.apache.iotdb.relational.it.query.old.query;

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
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBPaginationTableIT {
  private static final String DATABASE_NAME = "test";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE vehicle(device STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT)",
        "insert into vehicle(time,device,s0) values(1,'d1',101)",
        "insert into vehicle(time,device,s0) values(2,'d1',198)",
        "insert into vehicle(time,device,s0) values(100,'d1',99)",
        "insert into vehicle(time,device,s0) values(101,'d1',99)",
        "insert into vehicle(time,device,s0) values(102,'d1',80)",
        "insert into vehicle(time,device,s0) values(103,'d1',99)",
        "insert into vehicle(time,device,s0) values(104,'d1',90)",
        "insert into vehicle(time,device,s0) values(105,'d1',99)",
        "insert into vehicle(time,device,s0) values(106,'d1',99)",
        "insert into vehicle(time,device,s0) values(2,'d1',10000)",
        "insert into vehicle(time,device,s0) values(50,'d1',10000)",
        "insert into vehicle(time,device,s0) values(1000,'d1',22222)",
        "insert into vehicle(time,device,s1) values(1,'d1',1101)",
        "insert into vehicle(time,device,s1) values(2,'d1',198)",
        "insert into vehicle(time,device,s1) values(100,'d1',199)",
        "insert into vehicle(time,device,s1) values(101,'d1',199)",
        "insert into vehicle(time,device,s1) values(102,'d1',180)",
        "insert into vehicle(time,device,s1) values(103,'d1',199)",
        "insert into vehicle(time,device,s1) values(104,'d1',190)",
        "insert into vehicle(time,device,s1) values(105,'d1',199)",
        "insert into vehicle(time,device,s1) values(2,'d1',40000)",
        "insert into vehicle(time,device,s1) values(50,'d1',50000)",
        "insert into vehicle(time,device,s1) values(1000,'d1',55555)",
        "insert into vehicle(time,device,s2) values(1000,'d1',55555)",
        "insert into vehicle(time,device,s2) values(2,'d1',2.22)",
        "insert into vehicle(time,device,s2) values(3,'d1',3.33)",
        "insert into vehicle(time,device,s2) values(4,'d1',4.44)",
        "insert into vehicle(time,device,s2) values(102,'d1',10.00)",
        "insert into vehicle(time,device,s2) values(105,'d1',11.11)",
        "insert into vehicle(time,device,s2) values(1000,'d1',1000.11)",
        "insert into vehicle(time,device,s1) values(2000-01-01T08:00:00+08:00, 'd1',100)",
        "CREATE TABLE db(device STRING ID, s1 INT32 MEASUREMENT)",
        "insert into db(time,device,s1) values(0,'d1', 0)",
        "insert into db(time,device,s1) values(1,'d1', 1)",
        "insert into db(time,device,s1) values(2,'d1', 2)",
        "insert into db(time,device,s1) values(3,'d1', 3)",
        "insert into db(time,device,s1) values(4,'d1', 4)",
        "insert into db(time,device,s1) values(5,'d1', 5)",
        "insert into db(time,device,s1) values(6,'d1', 6)",
        "insert into db(time,device,s1) values(7,'d1', 7)",
        "insert into db(time,device,s1) values(8,'d1', 8)",
        "insert into db(time,device,s1) values(9,'d1', 9)",
        "insert into db(time,device,s1) values(10,'d1', 10)",
        "insert into db(time,device,s1) values(11,'d1', 11)",
        "insert into db(time,device,s1) values(12,'d1', 12)",
        "insert into db(time,device,s1) values(13,'d1', 13)",
        "insert into db(time,device,s1) values(14,'d1', 14)"
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
    prepareTableData(SQLs);
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
              defaultFormatDataTime(3) + ",null,",
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
  }
}
