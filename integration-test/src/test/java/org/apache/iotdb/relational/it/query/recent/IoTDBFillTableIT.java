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
public class IoTDBFillTableIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(device_id STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT, s5 BOOLEAN MEASUREMENT, s6 TEXT MEASUREMENT, s7 STRING MEASUREMENT, s8 BLOB MEASUREMENT, s9 TIMESTAMP MEASUREMENT, s10 DATE MEASUREMENT)",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) "
            + " values(1, 'd1', 1, 11, 1.1, 11.1, true, 'text1', 'string1', X'cafebabe01', 1, '2024-10-01')",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5) "
            + " values(2, 'd1', 2, 22, 2.2, 22.2, false)",
        "INSERT INTO table1(time,device_id,s6,s7,s8,s9,s10) "
            + " values(3, 'd1', 'text3', 'string3', X'cafebabe03', 3, '2024-10-03')",
        "INSERT INTO table1(time,device_id,s6,s7,s8,s9,s10) "
            + " values(4, 'd1', 'text4', 'string4', X'cafebabe04', 4, '2024-10-04')",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5) "
            + " values(5, 'd1', 5, 55, 5.5, 55.5, false)",
        "flush",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5) "
            + " values(7, 'd1', 7, 77, 7.7, 77.7, true)",
        "INSERT INTO table1(time,device_id,s6,s7,s8,s9,s10) "
            + " values(8, 'd1', 'text8', 'string8', X'cafebabe08', 8, '2024-10-08')",
        "CREATE TABLE table2(city STRING ID, device_id STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT)",
        "INSERT INTO table2(time,city,device_id,s2) values(1, 'shanghai', 'd1', 02111)",
        "INSERT INTO table2(time,city,device_id,s1) values(2, 'shanghai', 'd1', 0212)",
        "INSERT INTO table2(time,city,device_id,s2) values(1, 'beijing', 'd1', 01011)",
        "INSERT INTO table2(time,city,device_id,s1) values(2, 'beijing', 'd1', 0102)",
        "INSERT INTO table2(time,city,device_id,s1) values(3, 'beijing', 'd1', 0103)",
        "INSERT INTO table2(time,city,device_id,s1) values(4, 'beijing', 'd1', 0104)",
        "INSERT INTO table2(time,city,device_id,s1) values(1, 'beijing', 'd2', 0101)",
        "INSERT INTO table2(time,city,device_id,s2) values(2, 'beijing', 'd2', 01022)",
        "INSERT INTO table2(time,city,device_id,s2) values(3, 'beijing', 'd2', 01033)",
        "INSERT INTO table2(time,city,device_id,s2) values(4, 'beijing', 'd2', 01044)",
        "CREATE TABLE table3(city STRING ID, device_id STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT)",
        "INSERT INTO table3(time,city,device_id,s2) values(1, 'shanghai', 'd1', 02111)",
        "INSERT INTO table3(time,city,device_id,s1) values(2, 'shanghai', 'd1', 0212)",
        "INSERT INTO table3(time,city,device_id,s2) values(1, 'beijing', 'd1', 01011)",
        "INSERT INTO table3(time,city,device_id,s1) values(2, 'beijing', 'd1', 0102)",
        "INSERT INTO table3(time,city,device_id,s1,s2) values(3, 'beijing', 'd1', 0103,01033)",
        "INSERT INTO table3(time,city,device_id,s1) values(4, 'beijing', 'd1', 0104)",
        "INSERT INTO table3(time,city,device_id,s1) values(1, 'beijing', 'd2', 0101)",
        "INSERT INTO table3(time,city,device_id,s2) values(2, 'beijing', 'd2', 01022)",
        "INSERT INTO table3(time,city,device_id,s1,s2) values(3, 'beijing', 'd2', 0103, 01033)",
        "INSERT INTO table3(time,city,device_id,s2) values(4, 'beijing', 'd2', 01044)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void normalFillTest() {

    // --------------------------------- PREVIOUS FILL ---------------------------------

    // case 1: all without time filter using previous fill without timeDuration
    String[] expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.003Z,d1,2,22,2.2,22.2,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,2,22,2.2,22.2,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.008Z,d1,7,77,7.7,77.7,true,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD PREVIOUS", expectedHeader, retArray, DATABASE_NAME);

    // case 2: all with time filter using previous fill without timeDuration
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,null,null,null,null,null,",
          "1970-01-01T00:00:00.003Z,d1,2,22,2.2,22.2,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,2,22,2.2,22.2,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.008Z,d1,7,77,7.7,77.7,true,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 WHERE time > 1 FILL METHOD PREVIOUS",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 3: all with time filter and value filter using previous fill without timeDuration
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,null,null,null,null,null,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,null,null,null,null,null,",
        };
    tableResultSetEqualTest(
        "select * from table1 WHERE time > 1 and time < 8 and s2 > 22 FILL METHOD PREVIOUS",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 4: all without time filter using previous fill with timeDuration
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.003Z,d1,2,22,2.2,22.2,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,2,22,2.2,22.2,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,null,null,null,null,null,",
          "1970-01-01T00:00:00.008Z,d1,7,77,7.7,77.7,true,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD PREVIOUS TIME_BOUND 2ms",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 5: all without time filter using previous fill with timeDuration with helper column
    // index
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.003Z,d1,2,22,2.2,22.2,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,2,22,2.2,22.2,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,null,null,null,null,null,",
          "1970-01-01T00:00:00.008Z,d1,7,77,7.7,77.7,true,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD PREVIOUS TIME_BOUND 2ms TIME_COLUMN 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 6: all without time filter using previous fill with timeDuration with helper column
    // index
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,null,null,null,null,null,",
          "1970-01-01T00:00:00.003Z,d1,1,11,1.1,11.1,true,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,null,null,null,null,null,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,null,null,null,null,null,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,null,null,null,null,null,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD PREVIOUS TIME_BOUND 2ms TIME_COLUMN 11",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case7: all without time filter using previous fill with order by time desc
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.008Z,d1,7,77,7.7,77.7,true,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,null,null,null,null,null,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.004Z,d1,2,22,2.2,22.2,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.003Z,d1,2,22,2.2,22.2,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD PREVIOUS TIME_BOUND 2ms TIME_COLUMN 1 order by time desc",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case8: all without time filter using previous fill with order by value
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.008Z,d1,7,77,7.7,77.7,true,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.004Z,d1,2,22,2.2,22.2,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.003Z,d1,2,22,2.2,22.2,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,null,null,null,null,null,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD PREVIOUS TIME_BOUND 2ms TIME_COLUMN 1 order by s9 desc, time desc",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case9: all without time filter using previous fill with subQuery
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.003Z,d1,5,55,5.5,55.5,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,5,55,5.5,55.5,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,null,null,null,null,null,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from (select * from table1 order by time desc) FILL METHOD PREVIOUS TIME_BOUND 2ms order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case10: all without time filter using previous fill with FILL_GROUP
    expectedHeader = new String[] {"time", "city", "device_id", "s1", "s2"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,beijing,d1,null,1011,",
          "1970-01-01T00:00:00.002Z,beijing,d1,102,1011,",
          "1970-01-01T00:00:00.003Z,beijing,d1,103,1011,",
          "1970-01-01T00:00:00.004Z,beijing,d1,104,1011,",
          "1970-01-01T00:00:00.001Z,beijing,d2,101,null,",
          "1970-01-01T00:00:00.002Z,beijing,d2,101,1022,",
          "1970-01-01T00:00:00.003Z,beijing,d2,101,1033,",
          "1970-01-01T00:00:00.004Z,beijing,d2,101,1044,",
          "1970-01-01T00:00:00.001Z,shanghai,d1,null,2111,",
          "1970-01-01T00:00:00.002Z,shanghai,d1,212,2111,",
        };
    tableResultSetEqualTest(
        "select time,city,device_id,s1,s2 from table2 FILL METHOD PREVIOUS FILL_GROUP 2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case11: all without time filter using previous fill with FILL_GROUP and TIME_COLUMN
    tableResultSetEqualTest(
        "select time,city,device_id,s1,s2 from table2 FILL METHOD PREVIOUS TIME_COLUMN 1 FILL_GROUP 2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case12: all without time filter using previous fill with TIME_BOUND and FILL_GROUP
    expectedHeader = new String[] {"time", "city", "device_id", "s1", "s2"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,beijing,d1,null,1011,",
          "1970-01-01T00:00:00.002Z,beijing,d1,102,1011,",
          "1970-01-01T00:00:00.003Z,beijing,d1,103,1011,",
          "1970-01-01T00:00:00.004Z,beijing,d1,104,null,",
          "1970-01-01T00:00:00.001Z,beijing,d2,101,null,",
          "1970-01-01T00:00:00.002Z,beijing,d2,101,1022,",
          "1970-01-01T00:00:00.003Z,beijing,d2,101,1033,",
          "1970-01-01T00:00:00.004Z,beijing,d2,null,1044,",
          "1970-01-01T00:00:00.001Z,shanghai,d1,null,2111,",
          "1970-01-01T00:00:00.002Z,shanghai,d1,212,2111,",
        };
    tableResultSetEqualTest(
        "select time,city,device_id,s1,s2 from table2 FILL METHOD PREVIOUS TIME_BOUND 2ms FILL_GROUP 2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case13: all without time filter using previous fill with TIME_BOUND, FILL_GROUP and
    // TIME_COLUMN
    tableResultSetEqualTest(
        "select time,city,device_id,s1,s2 from table2 FILL METHOD PREVIOUS TIME_BOUND 2ms TIME_COLUMN 1 FILL_GROUP 2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // --------------------------------- LINEAR FILL ---------------------------------
    // case 1: all without time filter using linear fill
    expectedHeader =
        new String[] {"time", "device_id", "s1", "s2", "s3", "s5", "s6", "s7", "s8", "s9", "s10"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,false,null,null,null,1970-01-01T00:00:00.002Z,2024-10-02,",
          "1970-01-01T00:00:00.003Z,d1,3,33,3.3,null,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,4,44,4.4,null,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,false,null,null,null,1970-01-01T00:00:00.005Z,2024-10-05,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,true,null,null,null,1970-01-01T00:00:00.007Z,2024-10-07,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select time,device_id,s1,s2,s3,s5,s6,s7,s8,s9,s10 from table1 FILL METHOD LINEAR",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 2: all with time filter using linear fill
    expectedHeader =
        new String[] {"time", "device_id", "s1", "s2", "s3", "s5", "s6", "s7", "s8", "s9", "s10"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,false,null,null,null,null,null,",
          "1970-01-01T00:00:00.003Z,d1,3,33,3.3,null,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,4,44,4.4,null,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,false,null,null,null,1970-01-01T00:00:00.005Z,2024-10-05,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,true,null,null,null,1970-01-01T00:00:00.007Z,2024-10-07,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select time,device_id,s1,s2,s3,s5,s6,s7,s8,s9,s10 from table1 WHERE time > 1 FILL METHOD LINEAR",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 3: all with time filter and value filter using linear fill
    expectedHeader =
        new String[] {"time", "device_id", "s1", "s2", "s3", "s5", "s6", "s7", "s8", "s9", "s10"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,false,null,null,null,null,null,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,true,null,null,null,null,null,",
        };
    tableResultSetEqualTest(
        "select time,device_id,s1,s2,s3,s5,s6,s7,s8,s9,s10 from table1 WHERE time > 1 and time < 8 and s2 > 22 FILL METHOD LINEAR",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 5: all without time filter using linear fill with helper column
    // index
    expectedHeader =
        new String[] {"time", "device_id", "s1", "s2", "s3", "s5", "s6", "s7", "s8", "s9", "s10"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,false,null,null,null,1970-01-01T00:00:00.002Z,2024-10-02,",
          "1970-01-01T00:00:00.003Z,d1,3,33,3.3,null,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,4,44,4.4,null,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,false,null,null,null,1970-01-01T00:00:00.005Z,2024-10-05,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,true,null,null,null,1970-01-01T00:00:00.007Z,2024-10-07,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select time,device_id,s1,s2,s3,s5,s6,s7,s8,s9,s10 from table1 FILL METHOD LINEAR TIME_COLUMN 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 6: all without time filter using linear fill with helper column index
    expectedHeader =
        new String[] {"time", "device_id", "s1", "s2", "s3", "s5", "s6", "s7", "s8", "s9", "s10"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,false,null,null,null,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,null,null,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,null,null,null,null,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,false,null,null,null,null,null,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,true,null,null,null,null,null,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select time,device_id,s1,s2,s3,s5,s6,s7,s8,s9,s10 from table1 FILL METHOD LINEAR TIME_COLUMN 10",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case7: all without time filter using linear fill with order by time desc
    expectedHeader =
        new String[] {"time", "device_id", "s1", "s2", "s3", "s5", "s6", "s7", "s8", "s9", "s10"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,true,null,null,null,1970-01-01T00:00:00.007Z,2024-10-07,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,false,null,null,null,1970-01-01T00:00:00.005Z,2024-10-05,",
          "1970-01-01T00:00:00.004Z,d1,4,44,4.4,null,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.003Z,d1,3,33,3.3,null,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,false,null,null,null,1970-01-01T00:00:00.002Z,2024-10-02,",
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
        };
    tableResultSetEqualTest(
        "select time,device_id,s1,s2,s3,s5,s6,s7,s8,s9,s10 from table1 FILL METHOD LINEAR order by time desc",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case8: all without time filter using linear fill with order by value
    expectedHeader =
        new String[] {"time", "device_id", "s1", "s2", "s3", "s5", "s6", "s7", "s8", "s9", "s10"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,true,null,null,null,1970-01-01T00:00:00.007Z,2024-10-07,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,false,null,null,null,1970-01-01T00:00:00.005Z,2024-10-05,",
          "1970-01-01T00:00:00.004Z,d1,4,44,4.4,null,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.003Z,d1,3,33,3.3,null,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,false,null,null,null,1970-01-01T00:00:00.002Z,2024-10-02,",
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
        };
    tableResultSetEqualTest(
        "select time,device_id,s1,s2,s3,s5,s6,s7,s8,s9,s10 from table1 FILL METHOD LINEAR TIME_COLUMN 1 order by s9 desc, time desc",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case9: all without time filter using linear fill with subQuery
    expectedHeader =
        new String[] {"time", "device_id", "s1", "s2", "s3", "s5", "s6", "s7", "s8", "s9", "s10"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,false,null,null,null,1970-01-01T00:00:00.002Z,2024-10-02,",
          "1970-01-01T00:00:00.003Z,d1,3,33,3.3,null,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,4,44,4.4,null,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,false,null,null,null,1970-01-01T00:00:00.005Z,2024-10-05,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,true,null,null,null,1970-01-01T00:00:00.007Z,2024-10-07,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from (select time,device_id,s1,s2,s3,s5,s6,s7,s8,s9,s10 from table1 order by time desc) FILL METHOD LINEAR order by time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case10: all without time filter using linear fill with FILL_GROUP
    expectedHeader = new String[] {"time", "city", "device_id", "s1", "s2"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,beijing,d1,null,1011,",
          "1970-01-01T00:00:00.002Z,beijing,d1,102,1022,",
          "1970-01-01T00:00:00.003Z,beijing,d1,103,1033,",
          "1970-01-01T00:00:00.004Z,beijing,d1,104,null,",
          "1970-01-01T00:00:00.001Z,beijing,d2,101,null,",
          "1970-01-01T00:00:00.002Z,beijing,d2,102,1022,",
          "1970-01-01T00:00:00.003Z,beijing,d2,103,1033,",
          "1970-01-01T00:00:00.004Z,beijing,d2,null,1044,",
          "1970-01-01T00:00:00.001Z,shanghai,d1,null,2111,",
          "1970-01-01T00:00:00.002Z,shanghai,d1,212,null,",
        };
    tableResultSetEqualTest(
        "select time,city,device_id,s1,s2 from table3 FILL METHOD LINEAR FILL_GROUP 2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case11: all without time filter using linear fill with FILL_GROUP and TIME_COLUMN
    tableResultSetEqualTest(
        "select time,city,device_id,s1,s2 from table3 FILL METHOD LINEAR TIME_COLUMN 1 FILL_GROUP 2,3",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // --------------------------------- VALUE FILL ---------------------------------
    // case 1: fill with integer value
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,100,100,null,1970-01-01T00:00:00.100Z,0-01-00,",
          "1970-01-01T00:00:00.003Z,d1,100,100,100.0,100.0,true,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,100,100,100.0,100.0,true,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,100,100,null,1970-01-01T00:00:00.100Z,0-01-00,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,100,100,null,1970-01-01T00:00:00.100Z,0-01-00,",
          "1970-01-01T00:00:00.008Z,d1,100,100,100.0,100.0,true,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD CONSTANT 100", expectedHeader, retArray, DATABASE_NAME);

    // case 2: fill with float value
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,110.2,110.2,null,1970-01-01T00:00:00.110Z,0-01-10,",
          "1970-01-01T00:00:00.003Z,d1,110,110,110.2,110.2,true,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,110,110,110.2,110.2,true,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,110.2,110.2,null,1970-01-01T00:00:00.110Z,0-01-10,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,110.2,110.2,null,1970-01-01T00:00:00.110Z,0-01-10,",
          "1970-01-01T00:00:00.008Z,d1,110,110,110.2,110.2,true,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD CONSTANT 110.2", expectedHeader, retArray, DATABASE_NAME);

    // case 3: fill with boolean value
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,false,false,null,null,null,",
          "1970-01-01T00:00:00.003Z,d1,0,0,0.0,0.0,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,0,0,0.0,0.0,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,false,false,null,null,null,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,false,false,null,null,null,",
          "1970-01-01T00:00:00.008Z,d1,0,0,0.0,0.0,false,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD CONSTANT false", expectedHeader, retArray, DATABASE_NAME);

    // case 4: fill with string value
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,iotdb,iotdb,null,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,null,null,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,null,null,null,null,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,iotdb,iotdb,null,null,null,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,iotdb,iotdb,null,null,null,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,false,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD CONSTANT 'iotdb'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,2018-05-06,2018-05-06,null,2018-05-06T00:00:00.000Z,2018-05-06,",
          "1970-01-01T00:00:00.003Z,d1,null,null,null,null,false,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,null,null,null,null,false,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,2018-05-06,2018-05-06,null,2018-05-06T00:00:00.000Z,2018-05-06,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,2018-05-06,2018-05-06,null,2018-05-06T00:00:00.000Z,2018-05-06,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,false,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD CONSTANT '2018-05-06'",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // case 5: fill with blob value
    expectedHeader =
        new String[] {
          "time", "device_id", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"
        };
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,11,1.1,11.1,true,text1,string1,0xcafebabe01,1970-01-01T00:00:00.001Z,2024-10-01,",
          "1970-01-01T00:00:00.002Z,d1,2,22,2.2,22.2,false,0xcafebabe99,0xcafebabe99,0xcafebabe99,null,null,",
          "1970-01-01T00:00:00.003Z,d1,null,null,null,null,null,text3,string3,0xcafebabe03,1970-01-01T00:00:00.003Z,2024-10-03,",
          "1970-01-01T00:00:00.004Z,d1,null,null,null,null,null,text4,string4,0xcafebabe04,1970-01-01T00:00:00.004Z,2024-10-04,",
          "1970-01-01T00:00:00.005Z,d1,5,55,5.5,55.5,false,0xcafebabe99,0xcafebabe99,0xcafebabe99,null,null,",
          "1970-01-01T00:00:00.007Z,d1,7,77,7.7,77.7,true,0xcafebabe99,0xcafebabe99,0xcafebabe99,null,null,",
          "1970-01-01T00:00:00.008Z,d1,null,null,null,null,null,text8,string8,0xcafebabe08,1970-01-01T00:00:00.008Z,2024-10-08,",
        };
    tableResultSetEqualTest(
        "select * from table1 FILL METHOD CONSTANT X'cafebabe99'",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void abNormalFillTest() {

    // --------------------------------- PREVIOUS FILL ---------------------------------
    tableAssertTestFail(
        "select s1 from table1 FILL METHOD PREVIOUS TIME_COLUMN 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Don't need to specify TIME_COLUMN while either TIME_BOUND or FILL_GROUP parameter is not specified",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1 from table1 FILL METHOD PREVIOUS TIME_BOUND 2ms",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Cannot infer TIME_COLUMN for PREVIOUS FILL, there exists no column whose type is TIMESTAMP",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1 from table1 FILL METHOD PREVIOUS FILL_GROUP 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Cannot infer TIME_COLUMN for PREVIOUS FILL, there exists no column whose type is TIMESTAMP",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD PREVIOUS TIME_BOUND 2ms TIME_COLUMN 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Type of TIME_COLUMN for PREVIOUS FILL should only be TIMESTAMP, but type of the column you specify is INT32",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD PREVIOUS TIME_BOUND 2ms TIME_COLUMN 0",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": PREVIOUS FILL TIME_COLUMN position 0 is not in select list",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD PREVIOUS TIME_BOUND 2ms TIME_COLUMN 3",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": PREVIOUS FILL TIME_COLUMN position 3 is not in select list",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD PREVIOUS FILL_GROUP 0",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": PREVIOUS FILL FILL_GROUP position 0 is not in select list",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD PREVIOUS FILL_GROUP 3",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": PREVIOUS FILL FILL_GROUP position 3 is not in select list",
        DATABASE_NAME);

    // --------------------------------- LINEAR FILL ---------------------------------
    tableAssertTestFail(
        "select s1 from table1 FILL METHOD LINEAR",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Cannot infer TIME_COLUMN for LINEAR FILL, there exists no column whose type is TIMESTAMP",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD LINEAR TIME_COLUMN 1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Type of TIME_COLUMN for LINEAR FILL should only be TIMESTAMP, but type of the column you specify is INT32",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD LINEAR TIME_COLUMN 0",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": LINEAR FILL TIME_COLUMN position 0 is not in select list",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD LINEAR TIME_COLUMN 3",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": LINEAR FILL TIME_COLUMN position 3 is not in select list",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD LINEAR FILL_GROUP 0",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": LINEAR FILL FILL_GROUP position 0 is not in select list",
        DATABASE_NAME);

    tableAssertTestFail(
        "select s1, time from table1 FILL METHOD LINEAR FILL_GROUP 3",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": LINEAR FILL FILL_GROUP position 3 is not in select list",
        DATABASE_NAME);
  }
}
