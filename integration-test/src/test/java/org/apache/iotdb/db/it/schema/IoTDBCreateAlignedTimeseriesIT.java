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
package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCreateAlignedTimeseriesIT {

  private Statement statement;
  private Connection connection;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();

    connection = EnvFactory.getEnv().getConnection();
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  public void testCreateAlignedTimeseries() throws Exception {
    String[] timeSeriesArray =
        new String[] {
          "root.sg1.d1.vector1.s1,FLOAT,PLAIN,UNCOMPRESSED",
          "root.sg1.d1.vector1.s2,INT64,RLE,SNAPPY"
        };

    statement.execute("SET STORAGE GROUP TO root.sg1");
    try {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg1.d1.vector1(s1 FLOAT encoding=PLAIN compressor=UNCOMPRESSED,s2 INT64 encoding=RLE)");
    } catch (SQLException ignored) {
    }

    // ensure that current storage group in cache is right.
    assertTimeseriesEquals(timeSeriesArray);

    statement.close();
    connection.close();
    // todo test restart
    //    EnvironmentUtils.stopDaemon();
    //    setUp();
    //
    //    // ensure storage group in cache is right after recovering.
    //    assertTimeseriesEquals(timeSeriesArray);
  }

  @Ignore
  @Test
  public void testCreateAlignedTimeseriesWithDeletion() throws Exception {
    String[] timeSeriesArray =
        new String[] {
          "root.sg1.d1.vector1.s1,DOUBLE,PLAIN,SNAPPY", "root.sg1.d1.vector1.s2,INT64,RLE,SNAPPY"
        };

    statement.execute("SET STORAGE GROUP TO root.sg1");
    try {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg1.d1.vector1(s1 FLOAT encoding=PLAIN compressor=UNCOMPRESSED,s2 INT64 encoding=RLE)");
      statement.execute("DELETE TIMESERIES root.sg1.d1.vector1.s1");
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg1.d1.vector1(s1 DOUBLE encoding=PLAIN compressor=SNAPPY)");
    } catch (SQLException e) {
      e.printStackTrace();
    }

    // ensure that current storage group in cache is right.
    assertTimeseriesEquals(timeSeriesArray);

    statement.close();
    connection.close();
    // todo
    //    EnvironmentUtils.stopDaemon();
    setUp();

    // ensure storage group in cache is right after recovering.
    assertTimeseriesEquals(timeSeriesArray);
  }

  private void assertTimeseriesEquals(String[] timeSeriesArray) throws SQLException {

    int count = 0;
    try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES")) {
      while (resultSet.next()) {
        String ActualResult =
            resultSet.getString("timeseries")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression");
        Assert.assertEquals(timeSeriesArray[count], ActualResult);
        count++;
      }
    }
    Assert.assertEquals(timeSeriesArray.length, count);
  }
}
