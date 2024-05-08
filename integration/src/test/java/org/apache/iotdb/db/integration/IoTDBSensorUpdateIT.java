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

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBSensorUpdateIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testMerge() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.demo");
      statement.execute("create timeseries root.demo.d1.s1 with datatype=INT64,encoding=RLE");
      statement.execute("create timeseries root.demo.d1.s2 with datatype=INT64,encoding=RLE");
      statement.execute("create timeseries root.demo.d1.s3 with datatype=INT64,encoding=RLE");
      statement.execute("insert into root.demo.d1(time,s1,s2) values(1,1,2)");
      statement.execute("flush");
      statement.execute("insert into root.demo.d1(time,s3) values(1,1)");
      statement.execute("flush");
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.**")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
          assertEquals(1, set.getLong("root.demo.d1.s3"));
        }
        assertEquals(1, cnt);
      }
      statement.execute("merge");
      Thread.sleep(1000);
      // before merge completes
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.**")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
          assertEquals(1, set.getLong("root.demo.d1.s3"));
        }
        assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
