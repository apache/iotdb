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
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignedLastQueryWithTimeFilterIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableLastCache(false)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setTargetChunkPointNum(2);
    EnvFactory.getEnv().initClusterEnvironment();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database root.last_query_filter");
      statement.execute(
          "create aligned timeseries root.last_query_filter.d1("
              + "s1 INT32 encoding=RLE, s2 INT32 encoding=RLE, s3 INT32 encoding=RLE)");
      // The queried sensors only have values in the first chunk, while s3 extends the aligned
      // time-series statistics into the query's time range.
      statement.execute("insert into root.last_query_filter.d1(time,s1,s2) aligned values(1,1,11)");
      statement.execute("insert into root.last_query_filter.d1(time,s1,s2) aligned values(2,2,22)");
      statement.execute("insert into root.last_query_filter.d1(time,s3) aligned values(100,100)");
      statement.execute("insert into root.last_query_filter.d1(time,s3) aligned values(101,101)");
      statement.execute("flush");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testLastQuerySkipsFilteredSingletonAlignedChunk() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(10);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last s1,s2 from root.last_query_filter.d1 "
                  + "where time >= 100 and time <= 101")) {
        assertFalse(resultSet.next());
      }
    }
  }
}
