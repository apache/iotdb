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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBCreateAlignedTimeseriesIT extends AbstractSchemaIT {

  public IoTDBCreateAlignedTimeseriesIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void testCreateAlignedTimeseries() throws Exception {
    String[] timeSeriesArray =
        new String[] {
          "root.sg1.d1.vector1.s1,FLOAT,PLAIN,UNCOMPRESSED", "root.sg1.d1.vector1.s2,INT64,RLE,LZ4"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      try {
        statement.execute(
            "CREATE ALIGNED TIMESERIES root.sg1.d1.vector1(s1 FLOAT encoding=PLAIN compressor=UNCOMPRESSED,s2 INT64 encoding=RLE)");
      } catch (SQLException ignored) {
      }

      // ensure that current database in cache is right.
      assertTimeseriesEquals(timeSeriesArray);
    }
    // todo test restart
    //    EnvironmentUtils.stopDaemon();
    //    setUp();
    //
    //    // ensure database in cache is right after recovering.
    //    assertTimeseriesEquals(timeSeriesArray);
  }

  @Test
  public void testCreateAlignedTimeseriesWithDeletion() throws Exception {
    String[] timeSeriesArray =
        new String[] {
          "root.sg1.d1.vector1.s1,DOUBLE,PLAIN,SNAPPY", "root.sg1.d1.vector1.s2,INT64,RLE,LZ4"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      try {
        statement.execute(
            "CREATE ALIGNED TIMESERIES root.sg1.d1.vector1(s1 FLOAT encoding=PLAIN compressor=UNCOMPRESSED,s2 INT64 encoding=RLE)");
        statement.execute("DELETE TIMESERIES root.sg1.d1.vector1.s1");
        statement.execute(
            "CREATE ALIGNED TIMESERIES root.sg1.d1.vector1(s1 DOUBLE encoding=PLAIN compressor=SNAPPY)");
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    // ensure that current database in cache is right.
    assertTimeseriesEquals(timeSeriesArray);

    // todo
    //    EnvironmentUtils.stopDaemon();
    //    setUp();

    // ensure database in cache is right after recovering.
    assertTimeseriesEquals(timeSeriesArray);
  }

  private void assertTimeseriesEquals(String[] timeSeriesArray) throws SQLException {

    int count = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES")) {
      while (resultSet.next()) {
        String ActualResult =
            resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ENCODING)
                + ","
                + resultSet.getString(ColumnHeaderConstant.COMPRESSION);
        Assert.assertEquals(timeSeriesArray[count], ActualResult);
        count++;
      }
    }
    Assert.assertEquals(timeSeriesArray.length, count);
  }
}
