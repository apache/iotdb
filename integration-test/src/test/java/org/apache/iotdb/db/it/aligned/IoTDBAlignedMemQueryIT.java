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

package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.it.env.EnvFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

public class IoTDBAlignedMemQueryIT {

  @Before
  public void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("insert into root.vehicle.d0(time,s0) aligned values (10,310)");
      statement.execute("insert into root.vehicle.d0(time,s3) aligned values (10,'text')");
      statement.execute("insert into root.vehicle.d0(time,s4) aligned values (10,true)");

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1, cnt);
      }

      statement.execute("insert into root.vehicle.d1(time,s0,s1) aligned values (1,1,1)");
      statement.execute("insert into root.vehicle.d1(time,s0) aligned values (2,2)");
      statement.execute("insert into root.vehicle.d1(time,s1) aligned values (3,3)");

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d1")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(2, cnt);
      }
      statement.execute("flush");

      statement.execute("insert into root.vehicle.d1(time,s0,s1) aligned values (1,1,1)");
      statement.execute("insert into root.vehicle.d1(time,s0) aligned values (2,2)");
      statement.execute("insert into root.vehicle.d1(time,s1) aligned values (3,3)");

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d1")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(2, cnt);
      }
    }
  }
}
