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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class IoTDBTimePartitionIT {

  private long prevPartitionInterval;

  @Before
  public void setUp() throws ClassNotFoundException {
    EnvironmentUtils.closeStatMonitor();
    IoTDBDescriptor.getInstance().getConfig().setEnablePartition(true);
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(2592000);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
    IoTDBDescriptor.getInstance().getConfig().setEnablePartition(false);
  }

  @Test
  public void testOrderByTimeDesc() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] sqls =
          new String[] {
            "insert into root.group_1.d_1(timestamp, s_1) values(2018-07-18T00:00:00.000+08:00, 18.0)",
            "insert into root.group_1.d_1(timestamp, s_1) values(2018-07-19T00:00:00.000+08:00, 19.0)",
            "insert into root.group_1.d_1(timestamp, s_1) values(2019-08-19T00:00:00.000+08:00, 20.0)"
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }

      String[] retArray = new String[] {"20.0", "19.0", "18.0"};
      boolean hasResultSet = statement.execute("select * from root.group_1.d_1 order by time desc");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString("root.group_1.d_1.s_1");
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
