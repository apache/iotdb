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
package org.apache.iotdb.db.integration.aligned;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.sum;

public class IoTDBGroupByQueryWithoutValueFilterIT {

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;
  private static long prevPartitionInterval;

  private static final String TIMESTAMP_STR = "Time";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    // TODO When the aligned time series support compaction, we need to set compaction to true
    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void countSumAvgTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,4,40.0,7.5", "11,10,130142.0,13014.2", "21,1,null,230000.0", "31,0,355.0,null"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 where time > 5 GROUP BY ([1, 41), 10ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 where time > 5 GROUP BY ([1, 41), 10ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }
}
