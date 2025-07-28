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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.RemoteIT;

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
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class, RemoteIT.class})
public class IoTDBExecuteBatchIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testJDBCExecuteBatch() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.addBatch(
          "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,1.2)");
      statement.addBatch(
          "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600001,2.3)");
      statement.addBatch("delete timeseries root.ln.wf01.wt01.**");
      statement.addBatch(
          "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600002,3.4)");
      statement.executeBatch();
      ResultSet resultSet = statement.executeQuery("select * from root.ln.wf01.wt01");
      int count = 0;

      String[] timestamps = {"1509465600002"};
      String[] values = {"3.4"};

      while (resultSet.next()) {
        assertEquals(timestamps[count], resultSet.getString(ColumnHeaderConstant.TIME));
        assertEquals(values[count], resultSet.getString("root.ln.wf01.wt01.temperature"));
        count++;
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testJDBCExecuteBatchForCreateMultiTimeSeriesPlan() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(100);
      statement.addBatch(
          "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,1.2)");
      statement.addBatch(
          "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600001,2.3)");
      statement.addBatch("delete timeseries root.ln.wf01.wt01.**");
      statement.addBatch(
          "create timeseries root.turbine.d1.s1(s1) with datatype=boolean, encoding=plain , compression=snappy "
              + "tags('tag1'='v1', 'tag2'='v2') attributes('attr1'='v3', 'attr2'='v4')");
      statement.addBatch(
          "create timeseries root.turbine.d1.s2(s2) with datatype=float, encoding=rle, compression=uncompressed "
              + "tags('tag1'='v5', 'tag2'='v6') attributes('attr1'='v7', 'attr2'='v8') ");
      statement.addBatch(
          "insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600002,3.4)");
      statement.addBatch(
          "create timeseries root.turbine.d1.s3 with datatype=boolean, encoding=rle");
      statement.executeBatch();
      statement.clearBatch();
      ResultSet resultSet = statement.executeQuery("select * from root.ln.wf01.wt01");
      String[] timestamps = {"1509465600002"};
      String[] values = {"3.4"};
      int count = 0;
      while (resultSet.next()) {
        assertEquals(timestamps[count], resultSet.getString(ColumnHeaderConstant.TIME));
        assertEquals(values[count], resultSet.getString("root.ln.wf01.wt01.temperature"));
        count++;
      }
      ResultSet timeSeriesResultSetForS1 =
          statement.executeQuery("SHOW TIMESERIES root.turbine.d1.s1");
      count = 0;
      String[] key_s1 = {
        ColumnHeaderConstant.TIMESERIES,
        ColumnHeaderConstant.ALIAS,
        ColumnHeaderConstant.DATABASE,
        ColumnHeaderConstant.DATATYPE,
        ColumnHeaderConstant.ENCODING,
        ColumnHeaderConstant.COMPRESSION,
        ColumnHeaderConstant.TAGS,
        ColumnHeaderConstant.ATTRIBUTES
      };
      String[] value_s1 = {
        "root.turbine.d1.s1",
        "s1",
        "root.turbine",
        "BOOLEAN",
        "PLAIN",
        "SNAPPY",
        "{\"tag1\":\"v1\",\"tag2\":\"v2\"}",
        "{\"attr2\":\"v3\",\"attr1\":\"v4\"}"
      };

      while (timeSeriesResultSetForS1.next()) {
        assertEquals(value_s1[count], timeSeriesResultSetForS1.getString(key_s1[count]));
        count++;
      }

      ResultSet timeSeriesResultSetForS2 =
          statement.executeQuery("SHOW TIMESERIES root.turbine.d1.s2");
      count = 0;
      String[] key_s2 = {
        ColumnHeaderConstant.TIMESERIES,
        ColumnHeaderConstant.ALIAS,
        ColumnHeaderConstant.DATABASE,
        ColumnHeaderConstant.DATATYPE,
        ColumnHeaderConstant.ENCODING,
        ColumnHeaderConstant.COMPRESSION,
        ColumnHeaderConstant.TAGS,
        ColumnHeaderConstant.ATTRIBUTES
      };
      String[] value_s2 = {
        "root.turbine.d1.s2",
        "s2",
        "root.turbine",
        "FLOAT",
        "RLE",
        "UNCOMPRESSED",
        "{\"tag1\":\"v5\",\"tag2\":\"v6\"}",
        "{\"attr2\":\"v7\",\"attr1\":\"v8\"}"
      };
      while (timeSeriesResultSetForS2.next()) {
        assertEquals(value_s2[count], timeSeriesResultSetForS2.getString(key_s2[count]));
        count++;
      }

      count = 0;
      String[] key_s3 = {
        ColumnHeaderConstant.TIMESERIES,
        ColumnHeaderConstant.ALIAS,
        ColumnHeaderConstant.DATABASE,
        ColumnHeaderConstant.DATATYPE,
        ColumnHeaderConstant.ENCODING,
        ColumnHeaderConstant.COMPRESSION,
        ColumnHeaderConstant.TAGS,
        ColumnHeaderConstant.ATTRIBUTES
      };
      String[] value_s3 = {
        "root.turbine.d1.s3", "null", "root.turbine", "BOOLEAN", "RLE", "SNAPPY", "null", "null"
      };
      ResultSet timeSeriesResultSetForS3 =
          statement.executeQuery("SHOW TIMESERIES root.turbine.d1.s3");

      while (timeSeriesResultSetForS3.next()) {
        assertEquals(value_s3[count], timeSeriesResultSetForS3.getString(key_s3[count]));
        count++;
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }
}
