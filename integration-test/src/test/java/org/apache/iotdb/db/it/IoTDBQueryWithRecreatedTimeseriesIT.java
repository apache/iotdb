/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBQueryWithRecreatedTimeseriesIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  // https://issues.apache.org/jira/browse/IOTDB-2697
  public void testQueryDiffTypeTimeseries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg");
      Thread.sleep(100);
      statement.execute("CREATE TIMESERIES root.sg.d1.s1 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s2 with datatype=INT64,encoding=PLAIN");
      statement.execute("INSERT INTO root.sg.d1(time, s1, s2) VALUES(11000, 10, 20)");
      Thread.sleep(100);
      statement.execute("FLUSH");
      Thread.sleep(100);
      statement.execute("DELETE TIMESERIES root.sg.d1.s1");
      statement.execute("CREATE TIMESERIES root.sg.d1.s1 with datatype=INT32,encoding=PLAIN");
      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1 FROM root.sg.d1 WHERE s1 > 10")) {
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1 FROM root.sg.d1 WHERE s1 <= 10")) {
        Assert.assertFalse(resultSet.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
