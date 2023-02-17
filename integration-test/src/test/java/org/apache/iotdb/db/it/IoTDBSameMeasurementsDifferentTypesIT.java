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
package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.constant.TestConstant;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSameMeasurementsDifferentTypesIT {

  @BeforeClass
  public static void setUp() throws Exception {
    // use small page
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setMaxNumberOfPointsInPage(1000)
        .setPageSizeInByte(1024 * 150)
        .setGroupSizeInByte(1024 * 1000)
        .setMemtableSizeThreshold(1024 * 1000);

    EnvFactory.getEnv().initClusterEnvironment();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE root.fans");
      statement.execute("CREATE TIMESERIES root.fans.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.fans.d1.s0 WITH DATATYPE=INT64, ENCODING=RLE");

      for (int time = 1; time < 10; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 10);
        statement.execute(sql);
        sql = String.format("insert into root.fans.d1(timestamp,s0) values(%s,%s)", time, time % 5);
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllTest() {
    String[] retArray =
        new String[] {
          "1,1,1", "2,2,2", "3,3,3", "4,4,4", "5,5,0", "6,6,1", "7,7,2", "8,8,3", "9,9,4"
        };

    String selectSql = "select * from root.**";

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement1 = connection.createStatement();
        Statement statement2 = connection.createStatement()) {
      statement1.setFetchSize(10);
      ResultSet resultSet1 = statement1.executeQuery(selectSql);
      int cnt1 = 0;
      while (resultSet1.next() && cnt1 < 5) {
        String ans =
            resultSet1.getString(TestConstant.TIMESTAMP_STR)
                + ","
                + resultSet1.getString("root.fans.d0.s0")
                + ","
                + resultSet1.getString("root.fans.d1.s0");
        Assert.assertEquals(retArray[cnt1], ans);
        cnt1++;
      }

      statement2.setFetchSize(10);
      ResultSet resultSet2 = statement2.executeQuery(selectSql);
      int cnt2 = 0;
      while (resultSet2.next()) {
        String ans =
            resultSet2.getString(TestConstant.TIMESTAMP_STR)
                + ","
                + resultSet2.getString("root.fans.d0.s0")
                + ","
                + resultSet2.getString("root.fans.d1.s0");
        Assert.assertEquals(retArray[cnt2], ans);
        cnt2++;
      }
      Assert.assertEquals(9, cnt2);

      // use do-while instead of while because in the previous while loop, we have executed the next
      // function,
      // and the cursor has been moved to the next position, so we should fetch that value first.
      do {
        String ans =
            resultSet1.getString(TestConstant.TIMESTAMP_STR)
                + ","
                + resultSet1.getString("root.fans.d0.s0")
                + ","
                + resultSet1.getString("root.fans.d1.s0");
        Assert.assertEquals(retArray[cnt1], ans);
        cnt1++;
      } while (resultSet1.next());
      // Although the statement2 has the same sql as statement1, they shouldn't affect each other.
      // So the statement1's ResultSet should also have 9 rows in total.
      Assert.assertEquals(9, cnt1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
