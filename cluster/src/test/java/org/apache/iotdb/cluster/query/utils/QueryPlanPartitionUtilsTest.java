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

import static org.apache.iotdb.cluster.utils.Utils.insertData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.qp.executor.ClusterQueryProcessExecutor;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.utils.EnvironmentUtils;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryPlanPartitionUtilsTest {

  private Server server;
  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  private static final String LOCAL_ADDR = String
      .format("%s:%d", CLUSTER_CONFIG.getIp(), CLUSTER_CONFIG.getPort());
  private static ClusterRpcQueryManager manager = ClusterRpcQueryManager.getInstance();
  private ClusterQueryProcessExecutor queryDataExecutor = new ClusterQueryProcessExecutor();
  private QueryProcessor queryProcessor = new QueryProcessor(queryDataExecutor);

  private static final String URL = "127.0.0.1:6667/";

  private String[] createSQLs = {
      "set storage group to root.vehicle1",
      "set storage group to root.vehicle2",
      "set storage group to root.vehicle3",
      "CREATE TIMESERIES root.vehicle1.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle1.d0.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle2.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle2.d0.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle3.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle3.d0.s1 WITH DATATYPE=INT32, ENCODING=PLAIN"
  };
  private String[] insertSQLs = {
      "insert into root.vehicle1.d0(timestamp,s0) values(10,100)",
      "insert into root.vehicle1.d0(timestamp,s0,s1) values(200,100,102)",
      "insert into root.vehicle2.d0(timestamp,s0) values(10,120)",
      "insert into root.vehicle2.d0(timestamp,s0,s1) values(200,300,3102)",
      "insert into root.vehicle3.d0(timestamp,s1) values(100,100)",
      "insert into root.vehicle3.d0(timestamp,s0,s1) values(10,200,200)",
      "insert into root.vehicle3.d0(timestamp,s1) values(300,3000)"
  };

  private String[] queryStatementsWithoutFilters = {
      "select * from root",
      "select d0.s0 from root.vehicle2, root.vehicle3",
      "select vehicle1.d0.s1, vehicle2.d0.s1 from root"
  };
  private String[] queryStatementsWithFilters = {
      "select * from root where vehicle1.d0.s0 > 10 and (vehicle2.d0.s1 < 10001 or vehicle1.d0.s1 = 3)",
      "select * from root where not(vehicle1.d0.s0 < 10 and vehicle2.d0.s1 > 10001) or vehicle3.d0.s0 = 3",
      "select * from root.vehicle1 where d0.s0 > 10",
      "select * from root.vehicle2 where d0.s0 > 10 and time < 1555938482600",
  };
  private static final Map<Integer, QueryPlan> withoutFilterResults = new HashMap<>(4);
  private static final Map<Integer, QueryPlan> withFilterSelectResults = new HashMap<>(4);
  private static final Map<Integer, QueryPlan> withFilterFilterResults = new HashMap<>(4);
  private static final IExpression[] correntPruneFilterTree = new IExpression[2];

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

  private void initCorrectResult() throws Exception {

    QueryPlan queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select * from root.vehicle1,root.vehicle2, root.vehicle3");
    // without filter
    //1
    withoutFilterResults.put(1, queryPlan);

    //2
    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select vehicle2.d0.s0, vehicle3.d0.s0 from root, root");
    withoutFilterResults.put(2, queryPlan);

    //3
    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select vehicle1.d0.s1, vehicle2.d0.s1 from root");
    withoutFilterResults.put(3, queryPlan);

    // with filter
    //1
    queryPlan = (QueryPlan) queryProcessor.parseSQLToPhysicalPlan(
        "select * from root where vehicle1.d0.s0 > 10 and (vehicle2.d0.s1 < 10001 or vehicle1.d0.s1 = 3)");
    QueryExpression queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    IExpression optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    queryPlan.setExpression(optimizedExpression);
    withFilterSelectResults.put(1, queryPlan);

    queryPlan = (QueryPlan) queryProcessor.parseSQLToPhysicalPlan(
        "select vehicle1.d0.s0,vehicle2.d0.s1,vehicle1.d0.s1 from root where vehicle1.d0.s0 > 10 and (vehicle2.d0.s1 < 101 or vehicle1.d0.s1 = 3)");
    queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    withFilterFilterResults.put(1, queryPlan);

    //2
    queryPlan = (QueryPlan) queryProcessor.parseSQLToPhysicalPlan(
        "select * from root where not(vehicle1.d0.s0 < 10 and vehicle2.d0.s1 > 10001) or vehicle3.d0.s0 = 3");
    queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    withFilterSelectResults.put(2, queryPlan);
    queryPlan = (QueryPlan) queryProcessor.parseSQLToPhysicalPlan(
        "select vehicle1.d0.s0,vehicle2.d0.s1,vehicle3.d0.s0 from root where not(vehicle1.d0.s0 > 10 and vehicle2.d0.s1 < 101) and vehicle3.d0.s0 = 3");
    queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    withFilterFilterResults.put(2, queryPlan);

    //3
    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select * from root.vehicle1 where d0.s0 > 10");
    queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    withFilterSelectResults.put(3, queryPlan);

    queryPlan = (QueryPlan) queryProcessor.parseSQLToPhysicalPlan(
        "select d0.s0 from root.vehicle1 where d0.s0 > 10");
    queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    withFilterFilterResults.put(3, queryPlan);

    //4
    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select * from root.vehicle2 where d0.s0 > 10 and time < NOW()");
    queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    withFilterSelectResults.put(4, queryPlan);

    queryPlan = (QueryPlan) queryProcessor.parseSQLToPhysicalPlan(
        "select d0.s0 from root.vehicle2 where d0.s0 > 10 and time < NOW()");
    queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    withFilterFilterResults.put(4, queryPlan);
  }

  @Test
  public void splitQueryPlanWithoutValueFilter() throws Exception{
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      initCorrectResult();
      for(int i = 0 ; i < queryStatementsWithoutFilters.length; i++) {
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
            ClusterRpcSingleQueryManager singleQueryManager = ClusterRpcQueryManager.getInstance()
                .getSingleQuery(jobId);
            Map<String, QueryPlan> filterPathPlans = singleQueryManager.getFilterPathPlans();
            assertTrue(filterPathPlans.isEmpty());
            Map<String, QueryPlan> selectPathPlans = singleQueryManager.getSelectPathPlans();
            assertFalse(selectPathPlans.isEmpty());
            for(Entry<String, QueryPlan> entry1: selectPathPlans.entrySet()){
              QueryPlan queryPlan = entry1.getValue();
              QueryPlan correctQueryPlan = withoutFilterResults.get(i + 1);
              assertTrue(correctQueryPlan.getPaths().containsAll(queryPlan.getPaths()));
              assertEquals(correctQueryPlan.getExpression(),queryPlan.getExpression());
              assertEquals(correctQueryPlan.isQuery(), queryPlan.isQuery());
              assertEquals(correctQueryPlan.getOperatorType(), queryPlan.getOperatorType());
              assertEquals(correctQueryPlan.getAggregations(), queryPlan.getAggregations());
            }
          }
        }
      }
    }
  }

  @Test
  public void splitQueryPlanWithValueFilter() throws Exception{
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      initCorrectResult();
      for(int i = 0 ; i < queryStatementsWithFilters.length; i++) {
        String queryStatementsWithoutFilter = queryStatementsWithFilters[i];
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
            ClusterRpcSingleQueryManager singleQueryManager = ClusterRpcQueryManager.getInstance()
                .getSingleQuery(jobId);
            Map<String, QueryPlan> filterPathPlans = singleQueryManager.getFilterPathPlans();
            assertFalse(filterPathPlans.isEmpty());
            Map<String, QueryPlan> selectPathPlans = singleQueryManager.getSelectPathPlans();
            assertFalse(selectPathPlans.isEmpty());
            for(Entry<String, QueryPlan> entry1 : selectPathPlans.entrySet()) {
              QueryPlan queryPlan = entry1.getValue();
              QueryPlan correctQueryPlan = withFilterSelectResults.get(i + 1);
              assertTrue(correctQueryPlan.getPaths().containsAll(queryPlan.getPaths()));
              assertEquals(correctQueryPlan.getExpression().getType(), queryPlan.getExpression().getType());
              assertEquals(correctQueryPlan.isQuery(), queryPlan.isQuery());
              assertEquals(correctQueryPlan.getOperatorType(), queryPlan.getOperatorType());
              assertEquals(correctQueryPlan.getAggregations(), queryPlan.getAggregations());
            }
            for (Entry<String, QueryPlan> entry1 : filterPathPlans.entrySet()) {
              QueryPlan queryPlan = entry1.getValue();
              QueryPlan correctQueryPlan = withFilterFilterResults.get(i + 1);
              assertTrue(correctQueryPlan.getPaths().containsAll(queryPlan.getPaths()));
              assertEquals(correctQueryPlan.getExpression().getType(),
                  queryPlan.getExpression().getType());
              assertEquals(correctQueryPlan.isQuery(), queryPlan.isQuery());
              assertEquals(correctQueryPlan.getOperatorType(), queryPlan.getOperatorType());
              assertEquals(correctQueryPlan.getAggregations(), queryPlan.getAggregations());
            }
          }
        }
      }
    }
  }
}