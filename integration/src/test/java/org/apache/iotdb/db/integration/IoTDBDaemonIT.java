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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.d0;
import static org.apache.iotdb.db.constant.TestConstant.d1;
import static org.apache.iotdb.db.constant.TestConstant.s0;
import static org.apache.iotdb.db.constant.TestConstant.s1;
import static org.apache.iotdb.db.constant.TestConstant.s2;
import static org.apache.iotdb.db.constant.TestConstant.s3;
import static org.apache.iotdb.db.constant.TestConstant.s4;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBDaemonIT {

  private static String[] sqls =
      new String[] {
        "CREATE DATABASE root.vehicle.d0",
        "CREATE DATABASE root.vehicle.d1",
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
        "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
        "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
        "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",
        "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
        "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {

    EnvFactory.getEnv().cleanAfterClass();
  }

  private static void insertData() {
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
  public void selectWithDuplicatedColumnsTest1() {
    String[] retArray =
        new String[] {
          "1,101,101,1101,",
          "2,10000,10000,40000,",
          "50,10000,10000,50000,",
          "100,99,99,199,",
          "101,99,99,199,",
          "102,80,80,180,",
          "103,99,99,199,",
          "104,90,90,190,",
          "105,99,99,199,",
          "106,99,99,null,",
          "1000,22222,22222,55555,",
          "946684800000,null,null,100,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select s0,s0,s1 from root.vehicle.d0");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals(
            "Time,root.vehicle.d0.s0,root.vehicle.d0.s0,root.vehicle.d0.s1,", header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(12, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWithDuplicatedColumnsTest2() {
    String[] retArray = new String[] {"11,11,42988.0,11,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select count(s0),count(s0),sum(s0),count(s1) from root.vehicle.d0");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        assertEquals(
            "count(root.vehicle.d0.s0),count(root.vehicle.d0.s0),"
                + "sum(root.vehicle.d0.s0),count(root.vehicle.d0.s1),",
            header.toString());

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllSQLTest() {
    String[] retArray =
        new String[] {
          "1,101,1101,null,null,999",
          "2,10000,40000,2.22,null,null",
          "3,null,null,3.33,null,null",
          "4,null,null,4.44,null,null",
          "50,10000,50000,null,null,null",
          "60,null,null,null,aaaaa,null",
          "70,null,null,null,bbbbb,null",
          "80,null,null,null,ccccc,null",
          "100,99,199,null,null,null",
          "101,99,199,null,ddddd,null",
          "102,80,180,10.0,fffff,null",
          "103,99,199,null,null,null",
          "104,90,190,null,null,null",
          "105,99,199,11.11,null,null",
          "106,99,null,null,null,null",
          "1000,22222,55555,1000.11,null,888",
          "946684800000,null,100,null,good,null"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select * from root.**");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s0)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s1)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s2)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s3)
                  + ","
                  + resultSet.getString(d1 + IoTDBConstant.PATH_SEPARATOR + s0);
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(17, cnt);
      }

      retArray = new String[] {"100,true"};
      hasResultSet = statement.execute("select s4 from root.vehicle.d0");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s4);
          assertEquals(ans, retArray[cnt]);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWildCardSQLTest() {
    String[] retArray =
        new String[] {"2,2.22", "3,3.33", "4,4.44", "102,10.0", "105,11.11", "1000,1000.11"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select s2 from root.vehicle.*");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s2);
          assertEquals(ans, retArray[cnt]);
          cnt++;
        }
        assertEquals(6, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void dnfErrorSQLTest() {
    String[] retArray =
        new String[] {
          "1,101,1101",
          "2,10000,40000",
          "50,10000,50000",
          "100,99,199",
          "101,99,199",
          "102,80,180",
          "103,99,199",
          "104,90,190",
          "105,99,199"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s0,s1 from root.vehicle.d0 where time < 106 and (s0 >= 60 or s1 <= 200)");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s0)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s1);
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(9, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAndOperatorTest() {
    String[] retArray = new String[] {"1000,22222,55555,888"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // TODO select s0,s1 from root.vehicle.d0 where time > 106 and root.vehicle.d1.s0 > 100;
      boolean hasResultSet =
          statement.execute(
              "select d0.s0, d0.s1, d1.s0 from root.vehicle where time > 106 and root.vehicle.d0.s0 > 100");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s0)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s1)
                  + ","
                  + resultSet.getString(d1 + IoTDBConstant.PATH_SEPARATOR + s0);
          assertEquals(ans, retArray[cnt]);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAndOpeCrossTest() {
    String[] retArray = new String[] {"1000,22222,55555"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s0,s1 from root.vehicle.d0 where time > 106 and root.vehicle.d1.s0 > 100");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s0)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s1);
          assertEquals(ans, retArray[cnt]);
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectOneColumnWithFilterTest() {
    String[] retArray = new String[] {"102,180", "104,190", "946684800000,100"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      boolean hasTextMaxResultSet =
          statement.execute("select s1 from root.vehicle.d0 where s1 < 199");
      Assert.assertTrue(hasTextMaxResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(d0 + IoTDBConstant.PATH_SEPARATOR + s1);
          assertEquals(retArray[cnt++], ans);
        }
        assertEquals(3, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
