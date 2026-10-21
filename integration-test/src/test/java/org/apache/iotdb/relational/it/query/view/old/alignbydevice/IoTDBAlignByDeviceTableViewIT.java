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

package org.apache.iotdb.relational.it.query.view.old.alignbydevice;

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
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAlignByDeviceTableViewIT {

  private static final String DATABASE_NAME = "db";

  private static final String[] createTableViewSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create view vehicle(device_id STRING TAG, s0 INT32 FIELD, s1 INT64 FIELD, s2 FLOAT FIELD, s3 TEXT FIELD, s4 BOOLEAN FIELD) as root.vehicle.**"
      };

  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE root.vehicle",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
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
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T00:00:00+08:00, 100)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
        "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
        "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
        "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
        "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",
        "insert into root.vehicle.d0(timestamp,s3) values(60,'aaaaa')",
        "insert into root.vehicle.d0(timestamp,s3) values(70,'bbbbb')",
        "insert into root.vehicle.d0(timestamp,s3) values(80,'ccccc')",
        "insert into root.vehicle.d0(timestamp,s3) values(101,'ddddd')",
        "insert into root.vehicle.d0(timestamp,s3) values(102,'fffff')",
        "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T00:00:00+08:00, 'good')",
        "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
        "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
        "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
        "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : createTableViewSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s0", "s1", "s2", "s3", "s4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d0,101,1101,null,null,null,",
          "1970-01-01T00:00:00.002Z,d0,10000,40000,2.22,null,null,",
          "1970-01-01T00:00:00.003Z,d0,null,null,3.33,null,null,",
          "1970-01-01T00:00:00.004Z,d0,null,null,4.44,null,null,",
          "1970-01-01T00:00:00.050Z,d0,10000,50000,null,null,null,",
          "1970-01-01T00:00:00.060Z,d0,null,null,null,aaaaa,null,",
          "1970-01-01T00:00:00.070Z,d0,null,null,null,bbbbb,null,",
          "1970-01-01T00:00:00.080Z,d0,null,null,null,ccccc,null,",
          "1970-01-01T00:00:00.100Z,d0,99,199,null,null,true,",
          "1970-01-01T00:00:00.101Z,d0,99,199,null,ddddd,null,",
          "1970-01-01T00:00:00.102Z,d0,80,180,10.0,fffff,null,",
          "1970-01-01T00:00:00.103Z,d0,99,199,null,null,null,",
          "1970-01-01T00:00:00.104Z,d0,90,190,null,null,null,",
          "1970-01-01T00:00:00.105Z,d0,99,199,11.11,null,null,",
          "1970-01-01T00:00:00.106Z,d0,99,null,null,null,null,",
          "1970-01-01T00:00:01.000Z,d0,22222,55555,1000.11,null,null,",
          "1999-12-31T16:00:00.000Z,d0,null,100,null,good,null,",
          "1970-01-01T00:00:00.001Z,d1,999,null,null,null,null,",
          "1970-01-01T00:00:01.000Z,d1,888,null,null,null,null,",
        };
    tableResultSetEqualTest(
        "select * from vehicle order by device_id", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void selectTestWithLimitOffset1() {

    String[] expectedHeader = new String[] {"time", "device_id", "s0", "s1", "s2", "s3", "s4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,999,null,null,null,null,",
          "1970-01-01T00:00:00.002Z,d0,10000,40000,2.22,null,null,",
          "1970-01-01T00:00:00.003Z,d0,null,null,3.33,null,null,",
          "1970-01-01T00:00:00.004Z,d0,null,null,4.44,null,null,",
          "1970-01-01T00:00:00.050Z,d0,10000,50000,null,null,null,",
        };
    tableResultSetEqualTest(
        "select * from vehicle order by time asc, device_id offset 1 limit 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectTestWithLimitOffset2() {

    String[] expectedHeader = new String[] {"time", "device_id", "s0", "s1", "s2", "s3", "s4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,999,null,null,null,null,",
          "1999-12-31T16:00:00.000Z,d0,null,100,null,good,null,",
          "1970-01-01T00:00:01.000Z,d0,22222,55555,1000.11,null,null,",
          "1970-01-01T00:00:00.106Z,d0,99,null,null,null,null,",
          "1970-01-01T00:00:00.105Z,d0,99,199,11.11,null,null,",
        };
    tableResultSetEqualTest(
        "select * from vehicle order by device_id desc, time desc offset 1 limit 5",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectWithDuplicatedPathsTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s0", "s0", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d0,101,101,1101,",
          "1970-01-01T00:00:00.002Z,d0,10000,10000,40000,",
          "1970-01-01T00:00:00.050Z,d0,10000,10000,50000,",
          "1970-01-01T00:00:00.100Z,d0,99,99,199,",
          "1970-01-01T00:00:00.101Z,d0,99,99,199,",
          "1970-01-01T00:00:00.102Z,d0,80,80,180,",
          "1970-01-01T00:00:00.103Z,d0,99,99,199,",
          "1970-01-01T00:00:00.104Z,d0,90,90,190,",
          "1970-01-01T00:00:00.105Z,d0,99,99,199,",
          "1970-01-01T00:00:00.106Z,d0,99,99,null,",
          "1970-01-01T00:00:01.000Z,d0,22222,22222,55555,",
          "1999-12-31T16:00:00.000Z,d0,null,null,100,",
          "1970-01-01T00:00:00.001Z,d1,999,999,null,",
          "1970-01-01T00:00:01.000Z,d1,888,888,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s0,s0,s1 from vehicle where device_id = 'd0' or device_id = 'd1' order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectLimitTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s0", "s0", "s1"};

    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d0,10000,10000,40000,",
          "1970-01-01T00:00:00.050Z,d0,10000,10000,50000,",
          "1970-01-01T00:00:00.100Z,d0,99,99,199,",
          "1970-01-01T00:00:00.101Z,d0,99,99,199,",
          "1970-01-01T00:00:00.102Z,d0,80,80,180,",
          "1970-01-01T00:00:00.103Z,d0,99,99,199,",
          "1970-01-01T00:00:00.104Z,d0,90,90,190,",
          "1970-01-01T00:00:00.105Z,d0,99,99,199,",
          "1970-01-01T00:00:00.106Z,d0,99,99,null,",
          "1970-01-01T00:00:01.000Z,d0,22222,22222,55555,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s0,s0,s1 from vehicle order by device_id,time offset 1 limit 10",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectWithValueFilterTest() {

    String[] expectedHeader = new String[] {"time", "device_id", "s0", "s1", "s2", "s3", "s4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.100Z,d0,99,199,null,null,true,",
          "1970-01-01T00:00:00.101Z,d0,99,199,null,ddddd,null,",
          "1970-01-01T00:00:00.102Z,d0,80,180,10.0,fffff,null,",
          "1970-01-01T00:00:00.103Z,d0,99,199,null,null,null,",
          "1970-01-01T00:00:00.104Z,d0,90,190,null,null,null,",
          "1970-01-01T00:00:00.105Z,d0,99,199,11.11,null,null,",
        };
    tableResultSetEqualTest(
        "select * from vehicle where s0 > 0 AND s1 < 200 order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectDifferentSeriesWithValueFilterWithoutCacheTest() {

    String[] expectedHeader = new String[] {"time", "device_id", "s0"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.100Z,d0,99,",
          "1970-01-01T00:00:00.101Z,d0,99,",
          "1970-01-01T00:00:00.102Z,d0,80,",
          "1970-01-01T00:00:00.103Z,d0,99,",
          "1970-01-01T00:00:00.104Z,d0,90,",
          "1970-01-01T00:00:00.105Z,d0,99,",
          "1999-12-31T16:00:00.000Z,d0,null,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s0 from vehicle where s1 < 200 order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectDifferentSeriesWithBinaryValueFilterWithoutCacheTest() {

    String[] expectedHeader = new String[] {"time", "device_id", "s0"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.105Z,d0,99,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s0 from vehicle where s1 < 200 and s2 > 10  order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void predicateCannotNormalizedTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s0", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d0,101,1101,null,",
          "1970-01-01T00:00:00.002Z,d0,10000,40000,2.22,",
          "1970-01-01T00:00:00.050Z,d0,10000,50000,null,",
          "1970-01-01T00:00:00.100Z,d0,99,199,null,",
          "1970-01-01T00:00:00.101Z,d0,99,199,null,",
          "1970-01-01T00:00:00.103Z,d0,99,199,null,",
          "1970-01-01T00:00:00.105Z,d0,99,199,11.11,",
          "1970-01-01T00:00:01.000Z,d0,22222,55555,1000.11,",
        };
    tableResultSetEqualTest(
        "select time, device_id, s0,s1,s2 from vehicle where (((\"time\" > 10) AND (\"s1\" > 190)) OR (\"s2\" > 190.0) OR ((\"time\" < 4) AND (\"s1\" > 100))) order by device_id, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void duplicateProjectionsTest() {
    String[] expectedHeader = new String[] {"Time", "device_id", "_col2", "_col3", "alias"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d0,1102,1102,1102,",
          "1970-01-01T00:00:00.002Z,d0,40001,40001,40001,",
          "1970-01-01T00:00:00.050Z,d0,50001,50001,50001,",
          "1970-01-01T00:00:00.100Z,d0,200,200,200,",
          "1970-01-01T00:00:00.101Z,d0,200,200,200,",
          "1970-01-01T00:00:00.103Z,d0,200,200,200,",
          "1970-01-01T00:00:00.105Z,d0,200,200,200,",
          "1970-01-01T00:00:01.000Z,d0,55556,55556,55556,",
        };
    tableResultSetEqualTest(
        "select Time, device_id, s1+1, s1+1, s1+1 as alias from vehicle where (((\"time\" > 10) AND (\"s1\" > 190)) OR (\"s2\" > 190.0) OR ((\"time\" < 4) AND (\"s1\" > 100))) order by device_id, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  //
  //  @Test
  //  public void aggregateTest() {
  //    String[] retArray =
  //        new String[] {"root.vehicle.d0,11,11,6,6,1,", "root.vehicle.d1,2,null,null,null,null,"};
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select count(s0),count(s1),count(s2),count(s3),count(s4) "
  //                  + "from root.vehicle.d1,root.vehicle.d0 align by device")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        List<Integer> actualIndexToExpectedIndexList =
  //            checkHeader(
  //                resultSetMetaData,
  //                "Device,count(s0),count(s1),count(s2),count(s3),count(s4)",
  //                new int[] {
  //                    Types.VARCHAR,
  //                    Types.BIGINT,
  //                    Types.BIGINT,
  //                    Types.BIGINT,
  //                    Types.BIGINT,
  //                    Types.BIGINT,
  //                });
  //
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          String[] expectedStrings = retArray[cnt].split(",");
  //          StringBuilder expectedBuilder = new StringBuilder();
  //          StringBuilder actualBuilder = new StringBuilder();
  //          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //            actualBuilder.append(resultSet.getString(i)).append(",");
  //            expectedBuilder
  //                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
  //                .append(",");
  //          }
  //          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
  //          cnt++;
  //        }
  //        Assert.assertEquals(retArray.length, cnt);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void groupBytimeTest() {
  //    String[] retArray =
  //        new String[] {
  //            "2,root.vehicle.d0,1,1,3,0,0,",
  //            "22,root.vehicle.d0,0,0,0,0,0,",
  //            "42,root.vehicle.d0,0,0,0,0,0,",
  //            "2,root.vehicle.d1,0,null,null,null,null,",
  //            "22,root.vehicle.d1,0,null,null,null,null,",
  //            "42,root.vehicle.d1,0,null,null,null,null,"
  //        };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select count(*) from root.vehicle.** GROUP BY ([2,50),20ms) align by device")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        List<Integer> actualIndexToExpectedIndexList =
  //            checkHeader(
  //                resultSetMetaData,
  //                "time,Device,count(s0),count(s1),count(s2),count(s3),count(s4)",
  //                new int[] {
  //                    Types.tIMESTAMP,
  //                    Types.VARCHAR,
  //                    Types.BIGINT,
  //                    Types.BIGINT,
  //                    Types.BIGINT,
  //                    Types.BIGINT,
  //                    Types.BIGINT,
  //                });
  //
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          String[] expectedStrings = retArray[cnt].split(",");
  //          StringBuilder expectedBuilder = new StringBuilder();
  //          StringBuilder actualBuilder = new StringBuilder();
  //          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //            actualBuilder.append(resultSet.getString(i)).append(",");
  //            expectedBuilder
  //                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
  //                .append(",");
  //          }
  //          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
  //          cnt++;
  //        }
  //        Assert.assertEquals(retArray.length, cnt);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  public void groupBytimeWithValueFilterTest() {
  //    String[] retArray =
  //        new String[] {
  //            "2,root.vehicle.d0,2,", "102,root.vehicle.d0,1",
  //        };
  //
  //    try (Connection connection = EnvFactory.getEnv().getConnection();
  //        Statement statement = connection.createStatement()) {
  //
  //      try (ResultSet resultSet =
  //          statement.executeQuery(
  //              "select count(s2) from root.vehicle.d0 where s2 > 3 and s2 <= 10 GROUP BY
  // ([2,200),100ms) align by device")) {
  //        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
  //        List<Integer> actualIndexToExpectedIndexList =
  //            checkHeader(
  //                resultSetMetaData,
  //                "time,Device,count(s2)",
  //                new int[] {
  //                    Types.tIMESTAMP, Types.VARCHAR, Types.BIGINT,
  //                });
  //
  //        int cnt = 0;
  //        while (resultSet.next()) {
  //          String[] expectedStrings = retArray[cnt].split(",");
  //          StringBuilder expectedBuilder = new StringBuilder();
  //          StringBuilder actualBuilder = new StringBuilder();
  //          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
  //            actualBuilder.append(resultSet.getString(i)).append(",");
  //            expectedBuilder
  //                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
  //                .append(",");
  //          }
  //          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
  //          cnt++;
  //        }
  //        Assert.assertEquals(retArray.length, cnt);
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //      fail(e.getMessage());
  //    }
  //  }

  @Test
  public void unusualCaseTest2() {

    String[] expectedHeader =
        new String[] {"s0", "s0", "s1", "time", "device_id", "s0", "s1", "s2", "s3", "s4"};
    String[] retArray =
        new String[] {
          "101,101,1101,1970-01-01T00:00:00.001Z,d0,101,1101,null,null,null,",
          "10000,10000,40000,1970-01-01T00:00:00.002Z,d0,10000,40000,2.22,null,null,",
          "null,null,null,1970-01-01T00:00:00.003Z,d0,null,null,3.33,null,null,",
          "null,null,null,1970-01-01T00:00:00.004Z,d0,null,null,4.44,null,null,",
          "999,999,null,1970-01-01T00:00:00.001Z,d1,999,null,null,null,null,",
        };
    tableResultSetEqualTest(
        "select s0,s0,s1,* from vehicle where time < 20 and (device_id='d0' or device_id='d1') order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectWithRegularExpressionTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s0", "s1", "s2", "s3", "s4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d0,101,1101,null,null,null,",
          "1970-01-01T00:00:00.002Z,d0,10000,40000,2.22,null,null,",
          "1970-01-01T00:00:00.003Z,d0,null,null,3.33,null,null,",
          "1970-01-01T00:00:00.004Z,d0,null,null,4.44,null,null,",
          "1970-01-01T00:00:00.050Z,d0,10000,50000,null,null,null,",
          "1970-01-01T00:00:00.060Z,d0,null,null,null,aaaaa,null,",
          "1970-01-01T00:00:00.070Z,d0,null,null,null,bbbbb,null,",
          "1970-01-01T00:00:00.080Z,d0,null,null,null,ccccc,null,",
          "1970-01-01T00:00:00.100Z,d0,99,199,null,null,true,",
          "1970-01-01T00:00:00.101Z,d0,99,199,null,ddddd,null,",
          "1970-01-01T00:00:00.102Z,d0,80,180,10.0,fffff,null,",
          "1970-01-01T00:00:00.103Z,d0,99,199,null,null,null,",
          "1970-01-01T00:00:00.104Z,d0,90,190,null,null,null,",
          "1970-01-01T00:00:00.105Z,d0,99,199,11.11,null,null,",
          "1970-01-01T00:00:00.106Z,d0,99,null,null,null,null,",
          "1970-01-01T00:00:01.000Z,d0,22222,55555,1000.11,null,null,",
          "1999-12-31T16:00:00.000Z,d0,null,100,null,good,null,",
          "1970-01-01T00:00:00.001Z,d1,999,null,null,null,null,",
          "1970-01-01T00:00:01.000Z,d1,888,null,null,null,null,",
        };
    tableResultSetEqualTest(
        "select * from vehicle where device_id like 'd%' order by device_id",
        expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void selectWithNonExistMeasurementInWhereClause() {

    String[] expectedHeader = new String[] {"time", "device_id", "s0", "s1", "s2", "s3", "s4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d0,101,1101,null,null,null,",
        };
    tableResultSetEqualTest(
        "select * from vehicle where s1=1101 order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
