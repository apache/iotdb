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

import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 *
 * <p>You can comprehensively view the generated data in the following online doc:
 *
 * <p>https://docs.google.com/spreadsheets/d/1kfrSR1_paSd9B1Z0jnPBD3WQIMDslDuNm4R0mpWx9Ms/edit?usp=sharing
 */
@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBUnseqOverlappedPageIT {

  private static int beforeMaxNumberOfPointsInPage;

  private static String[] dataSet1 =
      new String[] {
        "CREATE DATABASE root.sg1",
        "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(time,s1) values(1, 1)",
        "INSERT INTO root.sg1.d1(time,s1) values(2, 2)",
        "INSERT INTO root.sg1.d1(time,s1) values(3, 3)",
        "INSERT INTO root.sg1.d1(time,s1) values(4, 4)",
        "INSERT INTO root.sg1.d1(time,s1) values(5, 5)",
        "INSERT INTO root.sg1.d1(time,s1) values(6, 6)",
        "flush",
        "INSERT INTO root.sg1.d1(time,s1) values(8, 8)",
        "INSERT INTO root.sg1.d1(time,s1) values(9, 9)",
        "INSERT INTO root.sg1.d1(time,s1) values(10, 10)",
        "INSERT INTO root.sg1.d1(time,s1) values(11, 11)",
        "INSERT INTO root.sg1.d1(time,s1) values(12, 12)",
        "INSERT INTO root.sg1.d1(time,s1) values(13, 13)",
        "flush",
        "INSERT INTO root.sg1.d1(time,s1) values(5, 50)",
        "INSERT INTO root.sg1.d1(time,s1) values(7, 70)",
        "INSERT INTO root.sg1.d1(time,s1) values(11, 110)",
        "INSERT INTO root.sg1.d1(time,s1) values(12, 120)",
        "flush",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    beforeMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    ConfigFactory.getConfig().setMaxNumberOfPointsInPage(3);
    EnvFactory.getEnv().initBeforeClass();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String insertSql : dataSet1) {
        statement.execute(insertSql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // recovery value
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig().setMemtableSizeThreshold(beforeMaxNumberOfPointsInPage);
  }

  @Test
  public void selectOverlappedPageTest() {
    String[] res = {
      "1,1", "2,2", "3,3", "4,4", "5,50", "6,6", "7,70", "8,8", "9,9", "10,10", "11,110", "12,120",
      "13,13"
    };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String sql = "select s1 from root.sg1.d1";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString("root.sg1.d1.s1");
          Assert.assertEquals(res[cnt], ans);
          cnt++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
