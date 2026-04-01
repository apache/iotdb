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

package org.apache.iotdb.relational.it.query.recent.extract;

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
public class IoTDBExtractTableIT {
  protected static final String DATABASE_NAME = "test";
  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(device_id STRING TAG, ts TIMESTAMP FIELD, s1 INT32 FIELD)",
        "INSERT INTO table1(time,device_id,ts,s1) values(2025/07/08 01:18:51.123456789,'d1',2025/07/08 01:18:51.123456789,1)",
        "INSERT INTO table1(time,device_id,ts,s1) values(2025/07/08 02:18:51.123456789,'d1',2025/07/08 02:18:51.123456789,2)",
        "INSERT INTO table1(time,device_id,ts,s1) values(2025/07/09 00:17:50.123456789,'d1',2025/07/09 00:17:50.123456789,3)",
        "FLUSH"
      };
  protected String decimal = "123";

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
  public void extractTest() {

    String[] expectedHeader =
        new String[] {
          "time", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8", "_col9",
          "_col10", "_col11"
        };
    String[] retArray =
        new String[] {getTimeStrUTC("2025-07-08T01:18:51") + ",2025,3,7,27,8,2,189,1,18,51,123,"};
    tableResultSetEqualTest(
        "SELECT time, extract(year from time),extract(quarter from time),extract(month from time),extract(week from time),extract(day from time),"
            + "extract(dow from time),extract(doy from time),extract(hour from time),extract(minute from time),extract(second from time),extract(ms from time)"
            + " FROM table1 order by time limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {getTimeStrUTC8("2025-07-08T09:18:51") + ",2025,3,7,27,8,2,189,9,18,51,123,"};
    tableResultSetEqualTest(
        "SELECT time,extract(year from time),extract(quarter from time),extract(month from time),extract(week from time),extract(day from time),"
            + "extract(dow from time),extract(doy from time),extract(hour from time),extract(minute from time),extract(second from time),extract(ms from time)"
            + " FROM table1 order by time limit 1",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void extractUsNsTest() {
    String[] expectedHeader = new String[] {"time", "_col1", "_col2"};
    String[] retArray =
        new String[] {
          getTimeStrUTC("2025-07-08T01:18:51") + ",0,0,",
        };
    tableResultSetEqualTest(
        "SELECT time, extract(us from time),extract(ns from time)"
            + " FROM table1 order by time limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          getTimeStrUTC8("2025-07-08T09:18:51") + ",0,0,",
        };
    tableResultSetEqualTest(
        "SELECT time, extract(us from time),extract(ns from time)"
            + " FROM table1 order by time limit 1",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void extractFilterPushDownTest() {
    String[] expectedHeader = new String[] {"time", "s1"};
    String[] retArray =
        new String[] {
          getTimeStrUTC("2025-07-08T02:18:51") + ",2,",
        };
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) > 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) > 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    // verify the pushdown result is same with non-pushdown
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) + 1 > 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) >= 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) >= 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    // verify the pushdown result is same with non-pushdown
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) + 1>= 3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          getTimeStrUTC8("2025-07-08T10:18:51") + ",2,",
        };
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) > 9",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) > 9",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) + 1 > 10",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) >= 10",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) >= 10",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts)+1>= 11",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "s1"};
    retArray =
        new String[] {
          getTimeStrUTC("2025-07-09T00:17:50") + ",3,",
        };
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) < 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) < 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) + 1< 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) <= 0",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) <= 0",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) +1<= 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) = 0",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) = 0",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) +1= 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    retArray =
        new String[] {
          getTimeStrUTC8("2025-07-09T08:17:50") + ",3,",
        };
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) < 9",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) < 9",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) + 1 < 10",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) <= 8",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) <= 8",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) +1<= 9",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) = 8",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) = 8",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) +1= 9",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          getTimeStrUTC("2025-07-08T01:18:51") + ",1,",
          getTimeStrUTC("2025-07-08T02:18:51") + ",2,",
        };
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) != 0",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) != 0",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) +1!= 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) between 1 and 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) between 1 and 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) +1 between 2 and 3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          getTimeStrUTC8("2025-07-08T09:18:51") + ",1,",
          getTimeStrUTC8("2025-07-08T10:18:51") + ",2,",
        };
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) != 8",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) != 8",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) +1 != 9",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from time) between 9 and 10",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) between 9 and 10",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, s1 FROM table1 where extract(hour from ts) +1 between 10 and 11",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testExtractFromComplexExpression() {
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray = new String[] {"0,"};
    tableResultSetEqualTest(
        "SELECT extract(hour from cast(s1 AS TIMESTAMP))" + " FROM table1 order by time limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testExtractFromConstant() {
    String[] expectedHeader = new String[] {"_col0"};
    String[] retArray = new String[] {"1,"};
    tableResultSetEqualTest(
        "SELECT extract(hour from 2025/07/08 01:18:51)" + " FROM table1 order by time limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  protected String getTimeStrUTC(String time) {
    return time + "." + decimal + 'Z';
  }

  protected String getTimeStrUTC8(String time) {
    return time + "." + decimal + "+08:00";
  }
}
