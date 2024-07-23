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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDuplicateTimeIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setAvgSeriesPointNumberThreshold(2);
    // Adjust memstable threshold size to make it flush automatically
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDuplicateTime() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db.d1.s1 with datatype=INT32,encoding=PLAIN");
      // version-1 tsfile
      statement.execute("insert into root.db.d1(time,s1) values (2,2)");
      statement.execute("insert into root.db.d1(time,s1) values (3,3)");

      // version-2 unseq work memtable
      statement.execute("insert into root.db.d1(time,s1) values (2,20)");

      // version-3 tsfile
      statement.execute("insert into root.db.d1(time,s1) values (5,5)");
      statement.execute("insert into root.db.d1(time,s1) values (6,6)");

      // version-2 unseq work memtable -> unseq tsfile
      statement.execute("insert into root.db.d1(time,s1) values (5,50)");

      try (ResultSet set = statement.executeQuery("SELECT s1 FROM root.db.d1 where time = 5")) {
        int cnt = 0;
        while (set.next()) {
          assertEquals(5L, set.getLong(1));
          assertEquals(50, set.getInt(2));

          cnt++;
        }
        assertEquals(1, cnt);
      }
    }
  }
}
