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
package org.apache.iotdb.db.integration;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBCompactionIT {

  private int prevSeqLevelFileNum;
  private int prevSeqLevelNum;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  @Test
  public void test() throws SQLException {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 0; i < 32; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(32, cnt);
    }
  }

}
