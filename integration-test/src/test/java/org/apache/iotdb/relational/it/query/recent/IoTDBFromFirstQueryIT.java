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
public class IoTDBFromFirstQueryIT {
  private static final String DATABASE_NAME = "test";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(device STRING TAG, region STRING TAG, temperature FLOAT FIELD, humidity DOUBLE FIELD)",
        "CREATE TABLE table2(device STRING TAG, location STRING TAG, pressure FLOAT FIELD)",
        "INSERT INTO table1(time,device,region,temperature,humidity) values(1,'d1','north',25.5,60.3)",
        "INSERT INTO table1(time,device,region,temperature,humidity) values(2,'d1','north',26.1,59.8)",
        "INSERT INTO table1(time,device,region,temperature,humidity) values(3,'d2','south',24.8,65.2)",
        "INSERT INTO table2(time,device,location,pressure) values(1,'d1','room1',1013.25)",
        "INSERT INTO table2(time,device,location,pressure) values(2,'d2','room2',1012.5)",
        "FLUSH"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testBasicFromFirstQuery() {
    String[] expectedHeader = new String[] {"time", "device", "region", "temperature", "humidity"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,north,25.5,60.3,",
          "1970-01-01T00:00:00.002Z,d1,north,26.1,59.8,",
          "1970-01-01T00:00:00.003Z,d2,south,24.8,65.2,"
        };

    tableResultSetEqualTest(
        "FROM table1 SELECT * order by time", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testFromFirstWithImplicitSelect() {
    String[] expectedHeader = new String[] {"time", "device", "region", "temperature", "humidity"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,north,25.5,60.3,",
          "1970-01-01T00:00:00.002Z,d1,north,26.1,59.8,",
          "1970-01-01T00:00:00.003Z,d2,south,24.8,65.2,"
        };

    tableResultSetEqualTest("FROM table1 order by time", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testFromFirstWithSimpleJoin() {
    String[] expectedHeader =
        new String[] {
          "time", "device", "region", "temperature", "humidity", "location", "pressure"
        };
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,north,25.5,60.3,room1,1013.25,",
          "1970-01-01T00:00:00.002Z,d1,north,26.1,59.8,null,null,",
          "1970-01-01T00:00:00.003Z,d2,south,24.8,65.2,null,null,"
        };

    tableResultSetEqualTest(
        "FROM table1 t1 LEFT JOIN table2 t2 ON t1.device = t2.device AND t1.time = t2.time "
            + "SELECT t1.time, t1.device, t1.region, t1.temperature, t1.humidity, t2.location, t2.pressure "
            + "ORDER BY t1.time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFromFirstWithWhereAndAggregate() {
    String[] expectedHeader = new String[] {"device", "avg_temp", "count_rows"};
    String[] retArray = new String[] {"d1,25.8,2,", "d2,24.8,1,"};

    tableResultSetEqualTest(
        "FROM table1 SELECT device, ROUND(AVG(temperature), 1) as avg_temp, COUNT(*) as count_rows WHERE temperature > 24.0 GROUP BY device order by device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
