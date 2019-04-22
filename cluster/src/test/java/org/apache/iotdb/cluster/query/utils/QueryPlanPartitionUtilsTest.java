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
package org.apache.iotdb.cluster.query.utils;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcQueryManager;
import org.apache.iotdb.cluster.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryPlanPartitionUtilsTest {

  private Server server;
  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  private static final String LOCAL_ADDR = String
      .format("%s:%d", CLUSTER_CONFIG.getIp(), CLUSTER_CONFIG.getPort());
  private static ClusterRpcQueryManager manager = ClusterRpcQueryManager.getInstance();

  private static final String URL = "127.0.0.1:6667/";

  private String[] createSQLs = {
      "set storage group to root.vehicle1",
      "set storage group to root.vehicle2",
      "set storage group to root.vehicle3",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle1.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle1.d0.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle2.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle2.d0.s1 WITH DATATYPE=INT32, ENCODING=PLAIN"
  };
  private String[] insertSQLs = {
      "insert into root.vehicle1.d0(timestamp,s0) values(10,100)",
      "insert into root.vehicle1.d0(timestamp,s0,s1) values(100,100,102)",
      "insert into root.vehicle2.d0(timestamp,s0) values(10,120)",
      "insert into root.vehicle2.d0(timestamp,s0,s1) values(200,300,3102)",
      "insert into root.vehicle3.d0(timestamp,s1) values(100,100)",
      "insert into root.vehicle3.d0(timestamp,s0,s1) values(10,200,200)",
      "insert into root.vehicle3.d0(timestamp,s1) values(300,3000)"
  };

  private String[] queryStatementsWithoutFilters = {"select * from root",
      "select * from root.vehicle1, root.vehicle2",
      "select root.vehicle.d0.s1, root.vehicle2.d0.s0 from root.vehicle1, root.vehicle2"
  };
  private String[] queryStatementsWithFilters = {"select * from root where vehicle1.d0.s0 > 10 and (vehicle2.d0.s1 < 101 or vehicle1.d0.s1 = 3)",
      "select * from root where not(vehicle1.d0.s0 > 10 and vehicle2.d0.s1 < 101) and vehicle3.d0.s0 = 3",
      "select * from root.vehicle1 where d0.s0 > 10",
  };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();
    CLUSTER_CONFIG.createAllPath();
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
  public void splitQueryPlanWithoutValueFilter() throws Exception{
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection);

      for(int i = 0 ; i < queryStatementsWithFilters.length; i++) {
        String queryStatementsWithoutFilter = queryStatementsWithoutFilters[i];
        try(Statement statement = connection.createStatement()) {
          boolean hasResultSet = statement.execute(queryStatementsWithoutFilter);
          assertTrue(hasResultSet);
          ResultSet resultSet = statement.getResultSet();
          assertTrue(resultSet.next());
          ConcurrentHashMap<Long, String> map = ClusterRpcQueryManager.getJobIdMapTaskId();
          assertEquals(1, map.size());
          for (String taskId : map.values()) {
            assertNotNull(manager.getSingleQuery(taskId));
          }
          for (long jobId : map.keySet()) {
            assertNotNull(manager.getSingleQuery(jobId));
          }
          for (Entry<Long, String> entry : map.entrySet()) {
            long jobId = entry.getKey();
            String taskId = entry.getValue();
            assertEquals(taskId, String.format("%s:%d", LOCAL_ADDR, jobId));
          }
        }
      }
    }
  }

  @Test
  public void splitQueryPlanWithValueFilter() {
  }


  private void insertData(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String createSql : createSQLs) {
        statement.execute(createSql);
      }
      for (String insertSql : insertSQLs) {
        statement.execute(insertSql);
      }
    }
  }
}