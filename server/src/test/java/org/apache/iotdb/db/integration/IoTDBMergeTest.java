/**
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

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBMergeTest {

  private static IoTDB daemon;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();

    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws SQLException {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.mergeTest");
      for (int i = 1; i <= 3; i++) {
        statement.execute("CREATE TIMESERIES root.mergeTest.s" + i + " WITH DATATYPE=INT64,"
            + "ENCODING=PLAIN");
      }

      for (int i = 0; i < 10; i++) {
        for (int j = i * 10 + 1; j <= (i+1) * 10; j++) {
          statement.execute(String.format("INSERT INTO root.mergeTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
              + "%d,%d)", j, j+1, j+2, j+3));
        }
        statement.execute("FLUSH");
        for (int j = i * 10 + 1; j <= (i+1) * 10; j++) {
          statement.execute(String.format("INSERT INTO root.mergeTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
              + "%d,%d)", j, j+10, j+20, j+30));
        }
        statement.execute("FLUSH");
        statement.execute("MERGE");

        ResultSet resultSet = statement.executeQuery("SELECT * FROM root.mergeTest");
        int cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.mergeTest.s1");
          long s2 = resultSet.getLong("root.mergeTest.s2");
          long s3 = resultSet.getLong("root.mergeTest.s3");
          assertEquals(time + 10, s1);
          assertEquals(time + 20, s2);
          assertEquals(time + 30, s3);
          cnt++;
        }
        assertEquals((i + 1) * 10, cnt);
      }
    }
  }

}
