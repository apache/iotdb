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

package org.apache.iotdb.db.it.fill;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBFillWithThresholdInNSIT {

  private static String[] creationSqls =
      new String[] {
        "CREATE DATABASE root.fillTest",
        "CREATE TIMESERIES root.fillTest.d0.s0 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.fillTest.d0.s1 WITH DATATYPE=INT64, ENCODING=PLAIN",
        "CREATE TIMESERIES root.fillTest.d0.s2 WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.fillTest.d0.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.fillTest.d0.s4 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.fillTest.d0.s5 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      };

  @Before
  public void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv().getConfig().getCommonConfig().setTimestampPrecision("ns");
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testFill() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] ans = {
        "1675223280000000000,4,3,4.0,4.0,on,false",
        "1675223340000000000,5,5,5.0,5.0,on,true",
        "1675223400000000000,6,null,6.0,6.0,null,false",
        "1675223460000000000,null,null,null,null,null,null",
        "1675223520000000000,null,null,null,null,null,null",
        "1675223580000000000,null,null,null,null,null,null",
        "1675223640000000000,null,null,null,null,null,null",
        "1675223700000000000,null,null,null,null,null,null",
        "1675223760000000000,7,7,7.0,null,off,false",
        "1675223820000000000,null,null,null,null,null,null",
        "1675223880000000000,8,8,8.0,null,on,true"
      };

      try (ResultSet set =
          statement.executeQuery(
              "SELECT last_value(*) FROM root.fillTest.d0 group by((2023-02-01T11:47:00.000000000+08:00, 2023-02-01T11:58:00.000000000+08:00], 1m)")) {
        int cnt = 0;
        while (set.next()) {
          String row =
              set.getString("Time")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s0)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s1)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s2)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s3)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s4)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s5)");

          assertEquals(ans[cnt], row);
          cnt++;
        }
        assertEquals(ans.length, cnt);
      }

      ans =
          new String[] {
            "1675223280000000000,4,3,4.0,4.0,on,false",
            "1675223340000000000,5,5,5.0,5.0,on,true",
            "1675223400000000000,6,5,6.0,6.0,on,false",
            "1675223460000000000,6,5,6.0,6.0,on,false",
            "1675223520000000000,6,5,6.0,6.0,on,false",
            "1675223580000000000,6,5,6.0,6.0,on,false",
            "1675223640000000000,6,5,6.0,6.0,on,false",
            "1675223700000000000,6,5,6.0,6.0,on,false",
            "1675223760000000000,7,7,7.0,6.0,off,false",
            "1675223820000000000,7,7,7.0,6.0,off,false",
            "1675223880000000000,8,8,8.0,6.0,on,true"
          };

      try (ResultSet set =
          statement.executeQuery(
              "SELECT last_value(*) FROM root.fillTest.d0 group by((2023-02-01T11:47:00.000000000+08:00, 2023-02-01T11:58:00.000000000+08:00], 1m) FILL(PREVIOUS)")) {
        int cnt = 0;
        while (set.next()) {
          String row =
              set.getString("Time")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s0)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s1)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s2)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s3)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s4)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s5)");
          assertEquals(ans[cnt], row);
          cnt++;
        }
        assertEquals(ans.length, cnt);
      }

      ans =
          new String[] {
            "1675223280000000000,4,3,4.0,4.0,on,false",
            "1675223340000000000,5,5,5.0,5.0,on,true",
            "1675223400000000000,6,5,6.0,6.0,on,false",
            "1675223460000000000,6,5,6.0,6.0,on,false",
            "1675223520000000000,6,null,6.0,6.0,null,false",
            "1675223580000000000,null,null,null,null,null,null",
            "1675223640000000000,null,null,null,null,null,null",
            "1675223700000000000,null,null,null,null,null,null",
            "1675223760000000000,7,7,7.0,null,off,false",
            "1675223820000000000,7,7,7.0,null,off,false",
            "1675223880000000000,8,8,8.0,null,on,true"
          };

      try (ResultSet set =
          statement.executeQuery(
              "SELECT last_value(*) FROM root.fillTest.d0 group by((2023-02-01T11:47:00.000000000+08:00, 2023-02-01T11:58:00.000000000+08:00], 1m) FILL(PREVIOUS, 2m)")) {
        int cnt = 0;
        while (set.next()) {
          String row =
              set.getString("Time")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s0)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s1)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s2)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s3)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s4)")
                  + ","
                  + set.getString("last_value(root.fillTest.d0.s5)");
          assertEquals(ans[cnt], row);
          cnt++;
        }
        assertEquals(ans.length, cnt);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  private void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
      statement.clearBatch();

      // 2023-02-01T11:47:30.000000000+08:00
      statement.addBatch(
          "INSERT INTO root.fillTest.d0(timestamp,s0,s1,s2,s3,s4,s5) VALUES(1675223250000000000, 1, 1, 1.0, null, 'on', true)");
      // 2023-02-01T11:47:40.000000001+08:00
      statement.addBatch(
          "INSERT INTO root.fillTest.d0(timestamp,s0,s1,s2,s3,s4,s5) VALUES(1675223260000000001, null, 2, 2.0, 2.0, null, false)");
      // 2023-02-01T11:47:50.000000100+08:00
      statement.addBatch(
          "INSERT INTO root.fillTest.d0(timestamp,s0,s1,s2,s3,s4,s5) VALUES(1675223270000000100, null, 3, null, 3.0, 'off', false)");
      // 2023-02-01T11:48:00.000000000+08:00
      statement.addBatch(
          "INSERT INTO root.fillTest.d0(timestamp,s0,s1,s2,s3,s4,s5) VALUES(1675223280000000000, 4, null, 4.0, 4.0, 'on', null)");
      // 2023-02-01T11:48:50.000000000+08:00
      statement.addBatch(
          "INSERT INTO root.fillTest.d0(timestamp,s0,s1,s2,s3,s4,s5) VALUES(1675223330000000000, 5, 5, 5.0, 5.0, 'on', true)");
      // 2023-02-01T11:49:00.000000001+08:00
      statement.addBatch(
          "INSERT INTO root.fillTest.d0(timestamp,s0,s1,s2,s3,s4,s5) VALUES(1675223340000000001, 6, null, 6.0, 6.0, null, false)");
      // 2023-02-01T11:55:50.000000000+08:00
      statement.addBatch(
          "INSERT INTO root.fillTest.d0(timestamp,s0,s1,s2,s3,s4,s5) VALUES(1675223750000000000, 7, 7, 7.0, null, 'off', false)");
      // 2023-02-01T11:57:50.000000000+08:00
      statement.addBatch(
          "INSERT INTO root.fillTest.d0(timestamp,s0,s1,s2,s3,s4,s5) VALUES(1675223870000000000, 8, 8, 8.0, null, 'on', true)");

      statement.executeBatch();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
