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
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.sum;

@Category({LocalStandaloneTest.class})
public class IoTDBGroupByFillQueryBigDataIT {

  private static final String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.sg1",
        "create aligned timeseries root.sg1.d1(s1 FLOAT encoding=RLE, s2 INT32 encoding=Gorilla compression=SNAPPY, s3 INT64)",
        "create timeseries root.sg1.d2.s1 WITH DATATYPE=FLOAT, encoding=RLE",
        "create timeseries root.sg1.d2.s2 WITH DATATYPE=INT32, encoding=Gorilla",
        "create timeseries root.sg1.d2.s3 WITH DATATYPE=INT64",
      };

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;
  protected static long prevPartitionInterval;

  private static final String TIMESTAMP_STR = "Time";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    // TODO When the aligned time series support compaction, we need to set compaction to true
    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    prevPartitionInterval =
        IoTDBDescriptor.getInstance().getConfig().getTimePartitionIntervalForStorage();
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(false);

    // set group by fill cache size to 0.1 MB for testing ElasticSerializableTVList
    IoTDBDescriptor.getInstance().getConfig().setGroupByFillCacheSizeInMB((float) 0.1);

    insertData();
  }

  public static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // create aligned and non-aligned time series
      for (String sql : sqls) {
        statement.execute(sql);
      }

      // insert aligned and non-aligned data
      for (int i = 0; i < 100000; i++) {
        statement.execute(
            String.format(
                "insert into root.sg1.d1(time, s2, s3) aligned values(%d, %d, %d)", i, i, i));
        statement.execute(
            String.format("insert into root.sg1.d2(time, s2, s3) values(%d, %d, %d)", i, i, i));
        if (i % 1000 == 0) {
          statement.execute("flush");
        }
      }
      statement.execute("insert into root.sg1.d1(time, s1) aligned values(0, 0)");
      statement.execute("insert into root.sg1.d1(time, s1) aligned values(99999, 99999)");
      statement.execute("insert into root.sg1.d2(time, s1) values(0, 0)");
      statement.execute("insert into root.sg1.d2(time, s1) values(99999, 99999)");
      statement.execute("flush");
      // wait flush
      Thread.sleep(100);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
    IoTDBDescriptor.getInstance().getConfig().setGroupByFillCacheSizeInMB((float) 1.0);
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void alignedBigDataLinearFillTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select avg(d1.s1), avg(d1.s2), avg(d1.s3), sum(d2.s1), sum(d2.s2), sum(d2.s3)  from root.sg1 "
                  + "GROUP BY ([0, 100000), 1ms) FILL (linear)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d2.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d2.s2"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d2.s3"));
          Assert.assertEquals(
              String.format("%d,%d.0,%d.0,%d.0,%d.0,%d.0,%d.0", cnt, cnt, cnt, cnt, cnt, cnt, cnt),
              ans);
          cnt++;
        }
        Assert.assertEquals(100000, cnt);
      }

      hasResultSet =
          statement.execute(
              "select avg(d1.s1), avg(d1.s2), avg(d1.s3), sum(d2.s1), sum(d2.s2), sum(d2.s3)  from root.sg1 "
                  + "GROUP BY ([0, 100000), 1ms) FILL (linear) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 99999;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d2.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d2.s2"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d2.s3"));
          Assert.assertEquals(
              String.format("%d,%d.0,%d.0,%d.0,%d.0,%d.0,%d.0", cnt, cnt, cnt, cnt, cnt, cnt, cnt),
              ans);
          cnt--;
        }
        Assert.assertEquals(-1, cnt);
      }
    }
  }
}
