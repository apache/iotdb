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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignByDeviceIT {

  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE root.vehicle",
        "CREATE DATABASE root.other",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.other.d1.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
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
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
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
        "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",
        "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
        "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
        "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
        "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",
        "insert into root.other.d1(timestamp,s0) values(2, 3.14)",
        "insert into root.other.d2(timestamp,s6) values(6, 6.66)",
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void selectTest() {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,null,null,null,",
          "2,root.vehicle.d0,10000,40000,2.22,null,null,",
          "3,root.vehicle.d0,null,null,3.33,null,null,",
          "4,root.vehicle.d0,null,null,4.44,null,null,",
          "50,root.vehicle.d0,10000,50000,null,null,null,",
          "60,root.vehicle.d0,null,null,null,aaaaa,null,",
          "70,root.vehicle.d0,null,null,null,bbbbb,null,",
          "80,root.vehicle.d0,null,null,null,ccccc,null,",
          "100,root.vehicle.d0,99,199,null,null,true,",
          "101,root.vehicle.d0,99,199,null,ddddd,null,",
          "102,root.vehicle.d0,80,180,10.0,fffff,null,",
          "103,root.vehicle.d0,99,199,null,null,null,",
          "104,root.vehicle.d0,90,190,null,null,null,",
          "105,root.vehicle.d0,99,199,11.11,null,null,",
          "106,root.vehicle.d0,99,null,null,null,null,",
          "1000,root.vehicle.d0,22222,55555,1000.11,null,null,",
          "946684800000,root.vehicle.d0,null,100,null,good,null,",
          "1,root.vehicle.d1,999,null,null,null,null,",
          "1000,root.vehicle.d1,888,null,null,null,null,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select * from root.vehicle.** align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectTestWithLimitOffset1() {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d1,999,null,null,null,null,",
          "2,root.vehicle.d0,10000,40000,2.22,null,null,",
          "3,root.vehicle.d0,null,null,3.33,null,null,",
          "4,root.vehicle.d0,null,null,4.44,null,null,",
          "50,root.vehicle.d0,10000,50000,null,null,null,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.vehicle.** order by time asc limit 5 offset 1 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectTestWithLimitOffset2() {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d1,999,null,null,null,null,",
          "946684800000,root.vehicle.d0,null,100,null,good,null,",
          "1000,root.vehicle.d0,22222,55555,1000.11,null,null,",
          "106,root.vehicle.d0,99,null,null,null,null,",
          "105,root.vehicle.d0,99,199,11.11,null,null,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.vehicle.** order by device desc, time desc limit 5 offset 1 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWithDuplicatedPathsTest() {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,101,1101,",
          "2,root.vehicle.d0,10000,10000,40000,",
          "50,root.vehicle.d0,10000,10000,50000,",
          "100,root.vehicle.d0,99,99,199,",
          "101,root.vehicle.d0,99,99,199,",
          "102,root.vehicle.d0,80,80,180,",
          "103,root.vehicle.d0,99,99,199,",
          "104,root.vehicle.d0,90,90,190,",
          "105,root.vehicle.d0,99,99,199,",
          "106,root.vehicle.d0,99,99,null,",
          "1000,root.vehicle.d0,22222,22222,55555,",
          "946684800000,root.vehicle.d0,null,null,100,",
          "1,root.vehicle.d1,999,999,null,",
          "1000,root.vehicle.d1,888,888,null,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s0,s0,s1 from root.vehicle.d0, root.vehicle.d1 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s0,s1",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.BIGINT
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectLimitTest() {
    String[] retArray =
        new String[] {
          "2,root.vehicle.d0,10000,10000,40000,",
          "50,root.vehicle.d0,10000,10000,50000,",
          "100,root.vehicle.d0,99,99,199,",
          "101,root.vehicle.d0,99,99,199,",
          "102,root.vehicle.d0,80,80,180,",
          "103,root.vehicle.d0,99,99,199,",
          "104,root.vehicle.d0,90,90,190,",
          "105,root.vehicle.d0,99,99,199,",
          "106,root.vehicle.d0,99,99,null,",
          "1000,root.vehicle.d0,22222,22222,55555,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s0,s0,s1 from root.vehicle.* limit 10 offset 1 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s0,s1",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.BIGINT
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSlimitTest2() {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,null,101,1101,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.vehicle.d0 limit 1 slimit 3 soffset 1 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s4,s0,s1",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN, Types.INTEGER, Types.BIGINT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSlimitTest() {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,1101,null,",
          "2,root.vehicle.d0,40000,2.22,",
          "3,root.vehicle.d0,null,3.33,",
          "4,root.vehicle.d0,null,4.44,",
          "50,root.vehicle.d0,50000,null,",
          "100,root.vehicle.d0,199,null,",
          "101,root.vehicle.d0,199,null,",
          "102,root.vehicle.d0,180,10.0,",
          "103,root.vehicle.d0,199,null,",
          "104,root.vehicle.d0,190,null,",
          "105,root.vehicle.d0,199,11.11,",
          "1000,root.vehicle.d0,55555,1000.11,",
          "946684800000,root.vehicle.d0,100,null,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s0,s1,s2 from root.vehicle.* slimit 2 soffset 1 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s1,s2",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.BIGINT, Types.FLOAT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWithValueFilterTest() {
    String[] retArray =
        new String[] {
          "100,root.vehicle.d0,99,199,null,null,true,",
          "101,root.vehicle.d0,99,199,null,ddddd,null,",
          "102,root.vehicle.d0,80,180,10.0,fffff,null,",
          "103,root.vehicle.d0,99,199,null,null,null,",
          "104,root.vehicle.d0,90,190,null,null,null,",
          "105,root.vehicle.d0,99,199,11.11,null,null,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.vehicle.* where s0 > 0 AND s1 < 200 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectDifferentSeriesWithValueFilterWithoutCacheTest() {
    String[] retArray =
        new String[] {
          "100,root.vehicle.d0,99,",
          "101,root.vehicle.d0,99,",
          "102,root.vehicle.d0,80,",
          "103,root.vehicle.d0,99,",
          "104,root.vehicle.d0,90,",
          "105,root.vehicle.d0,99,",
          "946684800000,root.vehicle.d0,null"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // statement.execute("CLEAR CACHE");
      // single device

      try (ResultSet resultSet =
          statement.executeQuery("select s0 from root.vehicle.d0 where s1 < 200 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0",
                new int[] {Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER});

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectDifferentSeriesWithBinaryValueFilterWithoutCacheTest() {
    String[] retArray =
        new String[] {
          "105,root.vehicle.d0,99,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // statement.execute("CLEAR CACHE");
      // single device

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s0 from root.vehicle.d0 where s1 < 200 and s2 > 10 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0",
                new int[] {Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER});

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void aggregateTest() {
    String[] retArray =
        new String[] {"root.vehicle.d0,11,11,6,6,1,", "root.vehicle.d1,2,null,null,null,null,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s0),count(s1),count(s2),count(s3),count(s4) "
                  + "from root.vehicle.d1,root.vehicle.d0 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Device,count(s0),count(s1),count(s2),count(s3),count(s4)",
                new int[] {
                  Types.VARCHAR,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeTest() {
    String[] retArray =
        new String[] {
          "2,root.vehicle.d0,1,1,3,0,0,",
          "22,root.vehicle.d0,0,0,0,0,0,",
          "42,root.vehicle.d0,0,0,0,0,0,",
          "2,root.vehicle.d1,0,null,null,null,null,",
          "22,root.vehicle.d1,0,null,null,null,null,",
          "42,root.vehicle.d1,0,null,null,null,null,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*) from root.vehicle.** GROUP BY ([2,50),20ms) align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,count(s0),count(s1),count(s2),count(s3),count(s4)",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeWithValueFilterTest() {
    String[] retArray =
        new String[] {
          "2,root.vehicle.d0,2,", "102,root.vehicle.d0,1",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s2) from root.vehicle.d0 where s2 > 3 and s2 <= 10 GROUP BY ([2,200),100ms) align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,count(s2)",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.BIGINT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void errorCaseTest3() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select s0 from root.*.* align by device");
      fail("No exception thrown.");
    } catch (Exception e) {
      // root.vehicle.d0.s0 INT32
      // root.vehicle.d1.s0 INT32
      // root.other.d1.s0 FLOAT
      System.out.println(e.getMessage());
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "data types of the same measurement column should be the same across devices."));
    }
  }

  /**
   * root.vehicle.d0.s0 INT32 root.vehicle.d1.s0 INT32 root.other.d1.s0 FLOAT but
   * count(root.vehicle.d0.s0) INT64 count(root.vehicle.d1.s0) INT64 count(root.other.d1.s0) INT64
   */
  @Test
  public void unusualCaseTest1() {
    String[] retArray =
        new String[] {"root.other.d1,1,", "root.vehicle.d0,11,", "root.vehicle.d1,2,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select count(s0) from root.*.* align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData, "Device,count(s0)", new int[] {Types.VARCHAR, Types.BIGINT});

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void unusualCaseTest2() {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,101,1101,101,1101,null,null,null,",
          "2,root.vehicle.d0,10000,10000,40000,10000,40000,2.22,null,null,",
          "3,root.vehicle.d0,null,null,null,null,null,3.33,null,null,",
          "4,root.vehicle.d0,null,null,null,null,null,4.44,null,null,",
          "1,root.vehicle.d1,999,999,null,999,null,null,null,null,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // duplicated devices

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s0,s0,s1,* from root.vehicle.*, root.vehicle.d0, root.vehicle.d1"
                  + " where time < 20 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s0,s1,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectNonExistTest() {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,null,null,null,null,",
          "2,root.vehicle.d0,10000,40000,2.22,null,null,null,",
          "3,root.vehicle.d0,null,null,3.33,null,null,null,",
          "4,root.vehicle.d0,null,null,4.44,null,null,null,",
          "50,root.vehicle.d0,10000,50000,null,null,null,null,",
          "60,root.vehicle.d0,null,null,null,aaaaa,null,null,",
          "70,root.vehicle.d0,null,null,null,bbbbb,null,null,",
          "80,root.vehicle.d0,null,null,null,ccccc,null,null,",
          "100,root.vehicle.d0,99,199,null,null,true,null,",
          "101,root.vehicle.d0,99,199,null,ddddd,null,null,",
          "102,root.vehicle.d0,80,180,10.0,fffff,null,null,",
          "103,root.vehicle.d0,99,199,null,null,null,null,",
          "104,root.vehicle.d0,90,190,null,null,null,null,",
          "105,root.vehicle.d0,99,199,11.11,null,null,null,",
          "106,root.vehicle.d0,99,null,null,null,null,null,",
          "1000,root.vehicle.d0,22222,55555,1000.11,null,null,null,",
          "946684800000,root.vehicle.d0,null,100,null,good,null,null,",
          "1,root.vehicle.d1,999,null,null,null,null,null,",
          "1000,root.vehicle.d1,888,null,null,null,null,null,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s0, s1, s2, s3, s4, s5 from root.vehicle.*  align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4,s5",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN, /* non exist column */
                  Types.VARCHAR
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private List<Integer> checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      Assert.assertNotNull(typeIndex);
      Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
      actualIndexToExpectedIndexList.add(typeIndex);
    }
    return actualIndexToExpectedIndexList;
  }

  @Test
  public void selectWithRegularExpressionTest() {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,null,null,null,",
          "2,root.vehicle.d0,10000,40000,2.22,null,null,",
          "3,root.vehicle.d0,null,null,3.33,null,null,",
          "4,root.vehicle.d0,null,null,4.44,null,null,",
          "50,root.vehicle.d0,10000,50000,null,null,null,",
          "60,root.vehicle.d0,null,null,null,aaaaa,null,",
          "70,root.vehicle.d0,null,null,null,bbbbb,null,",
          "80,root.vehicle.d0,null,null,null,ccccc,null,",
          "100,root.vehicle.d0,99,199,null,null,true,",
          "101,root.vehicle.d0,99,199,null,ddddd,null,",
          "102,root.vehicle.d0,80,180,10.0,fffff,null,",
          "103,root.vehicle.d0,99,199,null,null,null,",
          "104,root.vehicle.d0,90,190,null,null,null,",
          "105,root.vehicle.d0,99,199,11.11,null,null,",
          "106,root.vehicle.d0,99,null,null,null,null,",
          "1000,root.vehicle.d0,22222,55555,1000.11,null,null,",
          "946684800000,root.vehicle.d0,null,100,null,good,null,",
          "1,root.vehicle.d1,999,null,null,null,null,",
          "1000,root.vehicle.d1,888,null,null,null,null,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select * from root.vehicle.d* align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWithNonExistMeasurementInWhereClause() {
    String[] retArray = new String[] {"1,root.vehicle.d0,101,1101,null,null,null,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select * from root.vehicle.* where s1 == 1101 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * data structure D, time, s1, s2; d1: 1, 10, 20; d2: 1, 12, 22; d3: 1, null, 33
   *
   * <p>Query Sql: select s1 from root.good.** where s2 > 1 align by device
   */
  @Test
  public void removeDeviceWhereMeasurementWhenNoDeviceSelectTest() {
    String[] mockDataSql =
        new String[] {
          "create timeseries root.good.d1.s1 WITH DATATYPE=INT32;",
          "create timeseries root.good.d1.s2 WITH DATATYPE=INT32;",
          "create timeseries root.good.d2.s1 WITH DATATYPE=INT32;",
          "create timeseries root.good.d2.s2 WITH DATATYPE=INT32;",
          "create timeseries root.good.d3.s2 WITH DATATYPE=INT32;",
          "insert into root.good.d1(time, s1, s2) values(1, 10, 20);",
          "insert into root.good.d2(time, s1, s2) values(1, 12, 22);",
          "insert into root.good.d3(time, s2) values(1, 33);"
        };

    String[] retArray = new String[] {"1,root.good.d1,10", "1,root.good.d2,12"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : mockDataSql) {
        statement.execute(sql);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select s1 from root.good.** where s2 > 1 align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s1",
                new int[] {Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER});

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(
          "Meets exception in removeDeviceWhereMeasurementWhenNoDeviceSelectTest: "
              + e.getMessage());
    }
  }

  @Test
  public void nonExistMeasurementInHavingTest() {
    String[] retArray =
        new String[] {
          "1,root.other.d1,3.14,null,", "5,root.other.d2,null,6.66,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s0),last_value(s6) from root.other.** group by ([1,10),2ms) having last_value(s0) is not null or last_value(s6) is not null align by device")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,last_value(s0),last_value(s6)",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.FLOAT, Types.DOUBLE,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
