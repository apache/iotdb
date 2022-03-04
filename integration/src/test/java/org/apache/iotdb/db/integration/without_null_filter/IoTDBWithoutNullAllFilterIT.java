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
package org.apache.iotdb.db.integration.without_null_filter;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBWithoutNullAllFilterIT {

  private static String[] dataSet1 =
      new String[] {
          "SET STORAGE GROUP TO root.test",
          "CREATE TIMESERIES root.test.sg1.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg2.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg2.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",

          "INSERT INTO root.test.sg1(timestamp,s1,s2, s3) "
              + "values(1, true, 1, 1.0)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) "
              + "values(1, false, 1, 1.0)",

          "INSERT INTO root.test.sg1(timestamp, s2) "
              + "values(2, 2)",
          "INSERT INTO root.test.sg1(timestamp, s3) "
              + "values(2, 2.0)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) "
              + "values(2, true, 2, 2.0)",
          "flush",

          "INSERT INTO root.test.sg1(timestamp, s1) "
              + "values(3, false)",
          "INSERT INTO root.test.sg1(timestamp, s2) "
              + "values(5, 5)",
          "INSERT INTO root.test.sg1(timestamp, s3) "
              + "values(5, 5.0)",
          "INSERT INTO root.test.sg2(timestamp, s2) "
              + "values(5, 5)",
          "INSERT INTO root.test.sg2(timestamp, s3) "
              + "values(5, 5.0)",
          "flush",

          "INSERT INTO root.test.sg1(timestamp,s1,s2, s3) "
              + "values(6, true, 6, 6.0)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) "
              + "values(6, true, 6, 6.0)",
          "INSERT INTO root.test.sg1(timestamp, s1) "
              + "values(7, true)",
          "INSERT INTO root.test.sg1(timestamp, s3) "
              + "values(7, 7.0)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) "
              + "values(7, true, 7, 7.0)",
          "flush",

          "INSERT INTO root.test.sg1(timestamp, s1) "
              + "values(8, true)",
          "INSERT INTO root.test.sg1(timestamp, s3) "
              + "values(8, 8.0)",
          "INSERT INTO root.test.sg2(timestamp, s3) "
              + "values(8, 8.0)",
          "INSERT INTO root.test.sg1(timestamp,s1,s2, s3) "
              + "values(9, false, 9, 9.0)",
          "INSERT INTO root.test.sg2(timestamp, s1) "
              + "values(9, true)",
          "flush",

          "INSERT INTO root.test.sg2(timestamp, s2) "
              + "values(9, 9.0)",
          "INSERT INTO root.test.sg1(timestamp,s1,s2, s3) "
              + "values(10, true, 10, 10.0)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) "
              + "values(10, true, 10, 10.0)",
          "flush",
      };

  private static final String TIMESTAMP_STR = "Time";
  private long prevPartitionInterval;

  private void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet1) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setUp() throws Exception {
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    ConfigFactory.getConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initBeforeTest();
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
  }

  @Test
  public void rawDataWithoutValueFilterQueryTest() {
    System.out.println("rawDataWithoutValueFilterQueryTest");
    String[] retArray1 =
        new String[] {
            "1,true,1,1.0",
            "2,null,2,2.0",
            "5,null,5,5.0",
            "6,true,6,6.0",
            "7,true,null,7.0",
            "8,true,null,8.0",
            "9,false,9,9.0",
            "10,true,10,10.0"
        };
    String[] retArray2 =
        new String[] {
            "1,true,1,1.0",
            "3,false,null,null",
            "6,true,6,6.0",
            "7,true,null,7.0",
            "8,true,null,8.0",
            "9,false,9,9.0",
            "10,true,10,10.0"
        };
    String[] retArray3 =
        new String[] {
            "1,true,1,1.0",
            "2,null,2,2.0",
            "3,false,null,null",
            "5,null,5,5.0",
            "6,true,6,6.0",
            "7,true,null,7.0",
            "8,true,null,8.0",
            "9,false,9,9.0",
            "10,true,10,10.0"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select * from root.test.sg1 without null all (s2, s3)");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3");
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 without null all (s1)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.test.sg1.s1"))
                  + ","
                  + resultSet.getString(sum("root.test.sg1.s2"))
                  + ","
                  + resultSet.getString(avg("root.test.sg1.s3"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 without null all");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.test.sg1.s1"))
                  + ","
                  + resultSet.getString(sum("root.test.sg1.s2"))
                  + ","
                  + resultSet.getString(avg("root.test.sg1.s3"));
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void rawDataWithValueFilterQueryTest() {
    System.out.println("rawDataWithValueFilterQueryTest");
    String[] retArray1 =
        new String[] {
            "9,false,9,9.0"
        };
    String[] retArray2 =
        new String[] {
            "1,true,1,1.0"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select * from root.test.sg1 where s1 = false without null all (s2, s3)");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3");
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 where s2 = 1 without null all (s1)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.test.sg1.s1"))
                  + ","
                  + resultSet.getString(sum("root.test.sg1.s2"))
                  + ","
                  + resultSet.getString(avg("root.test.sg1.s3"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 where s2 = 2 without null all (s1)");

      Assert.assertFalse(hasResultSet);

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 where s1 = true without null all");

      Assert.assertFalse(hasResultSet);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void withExpressionQueryTest() {
    System.out.println("withExpressionQueryTest");
    String[] retArray1 =
        new String[] {
            "1,true,1,1.0",
            "2,null,2,2.0",
            "5,null,5,5.0",
            "6,true,6,6.0",
            "7,true,null,7.0",
            "8,true,null,8.0",
            "9,false,9,9.0",
            "10,true,10,10.0"
        };
    String[] retArray2 =
        new String[] {
            "1,true,1,1.0",
            "3,false,null,null",
            "6,true,6,6.0",
            "7,true,null,7.0",
            "8,true,null,8.0",
            "9,false,9,9.0",
            "10,true,10,10.0"
        };
    String[] retArray3 =
        new String[] {
            "1,true,1,1.0",
            "2,null,2,2.0",
            "3,false,null,null",
            "5,null,5,5.0",
            "6,true,6,6.0",
            "7,true,null,7.0",
            "8,true,null,8.0",
            "9,false,9,9.0",
            "10,true,10,10.0"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s1, - s1, s2, + s2, s1 + s2, s1 - s2, s1 * s2, s1 / s2, s1 % s2 from root.sg1.d1 without null all (s1+s2);");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3");
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select s1, - s1, s2, + s2, s1 + s2, s1 - s2, s1 * s2, s1 / s2, s1 % s2 from root.sg1.d1 without null all (s1+s2, s2)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.test.sg1.s1"))
                  + ","
                  + resultSet.getString(sum("root.test.sg1.s2"))
                  + ","
                  + resultSet.getString(avg("root.test.sg1.s3"));
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 without null all");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.test.sg1.s1"))
                  + ","
                  + resultSet.getString(sum("root.test.sg1.s2"))
                  + ","
                  + resultSet.getString(avg("root.test.sg1.s3"));
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void withAliasQueryTest() {}

  @Test
  public void withUDFQueryTest() {}

  @Test
  public void withGroupByTimeQueryTest() {}

  @Test
  public void withGroupByLevelQueryTest() {}

  @Test
  public void withoutNullColumnsIsFullPathQueryTest() {
    //
  }

  @Test
  public void withoutNullColumnsMisMatchSelectedQueryTest() {
    //
  }

  @Test
  public void alignByDeviceQueryTest() {

  }

  @Test
  public void lastValueQueryTest() {

  }

  @Test
  public void selectFromMultiPathQueryTest() {}

  @Test
  public void withLimitOffsetQueryTest() {}
}
