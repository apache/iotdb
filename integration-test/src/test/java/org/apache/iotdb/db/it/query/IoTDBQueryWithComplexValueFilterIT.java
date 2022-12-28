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
package org.apache.iotdb.db.it.query;

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
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBQueryWithComplexValueFilterIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRawQuery1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1 from root.sg1.d1 where (time > 4 and s1 <= 6) or (s2 > 3 and time <= 5)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRawQuery2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1 from root.sg1.d1 where (time > 4 and s1 <= 6) and (s2 > 3 and time <= 5)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database root.sg1");
      statement.execute("create timeseries root.sg1.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("create timeseries root.sg1.d1.s2 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(0,0,0)");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(1,1,1)");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(2,2,2)");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(3,3,3)");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(4,4,4)");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(5,5,5)");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(6,6,6)");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(7,7,7)");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(8,8,8)");
      statement.execute("insert into root.sg1.d1(time,s1,s2) values(9,9,9)");
      //      for (int i = 0; i < 10; i++) {
      //        statement.addBatch(
      //            String.format(
      //                "insert into root.sg1.d1(time,s1,s2) values(%d,%d,%f)", i, i, (double) i));
      //      }
      //      statement.executeBatch();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
