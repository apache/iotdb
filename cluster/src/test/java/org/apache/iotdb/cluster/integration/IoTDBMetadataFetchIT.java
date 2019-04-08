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
package org.apache.iotdb.cluster.integration;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBMetadataFetchIT {

  private Server server;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();
    server = Server.getInstance();
    server.start();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void showTimeseriesTest1() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      insertSQL(connection, false);
      checkCorretness(connection);
    } finally {
      connection.close();
    }
  }

//  @Test
  public void showTimeseriesTest1Batch() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      insertSQL(connection, true);
      checkCorretness(connection);
    } finally {
      connection.close();
    }
  }

  private void insertSQL(Connection connection, boolean isBatch) throws SQLException {
    Statement statement = connection.createStatement();
    String[] insertSqls = new String[]{"SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, "
            + "compressor = SNAPPY, MAX_POINT_NUMBER = 3"};
    if (isBatch) {
      for (String sql : insertSqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
      statement.clearBatch();
    } else {
      for (String sql : insertSqls) {
        statement.execute(sql);
      }
    }
    statement.close();
  }

  private void checkCorretness(Connection connection) throws SQLException{
    Statement statement = connection.createStatement();
    String[] sqls = new String[]{
        "show timeseries root.ln.wf01.wt01.status", // full seriesPath
        "show timeseries root.ln", // prefix seriesPath
        "show timeseries root.ln.*.wt01", // seriesPath with stars
        "show timeseries root.a.b", // nonexistent timeseries, thus returning ""
        "show timeseries root.ln,root.ln",
        // SHOW TIMESERIES <PATH> only accept single seriesPath, thus
        // returning ""
    };
    String[] standards = new String[]{
        "root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n",
        "root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n"
            + "root.ln.wf01.wt01.temperature,root.ln.wf01.wt01,FLOAT,RLE,\n",
        "root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n"
              + "root.ln.wf01.wt01.temperature,root.ln.wf01.wt01,FLOAT,RLE,\n",
        "",
        ""
    };
    for (int i = 0; i < sqls.length; i++) {
      String sql = sqls[i];
      String standard = standards[i];
      StringBuilder builder = new StringBuilder();
      try {
        boolean hasResultSet = statement.execute(sql);
        if (hasResultSet) {
          ResultSet resultSet = statement.getResultSet();
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int j = 1; j <= resultSetMetaData.getColumnCount(); j++) {
              builder.append(resultSet.getString(j)).append(",");
            }
            builder.append("\n");
          }
        }
        Assert.assertEquals(standard, builder.toString());
      } catch (SQLException e) {
        e.printStackTrace();
        fail();
      } finally {
        statement.close();
      }
    }

  }

}
