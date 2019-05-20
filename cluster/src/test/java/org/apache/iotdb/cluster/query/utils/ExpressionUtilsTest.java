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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.qp.executor.ClusterQueryProcessExecutor;
import org.apache.iotdb.cluster.qp.executor.QueryMetadataExecutor;
import org.apache.iotdb.cluster.query.expression.TrueExpression;
import org.apache.iotdb.cluster.utils.EnvironmentUtils;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExpressionUtilsTest {
  private Server server;
  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  private ClusterQueryProcessExecutor queryDataExecutor = new ClusterQueryProcessExecutor(
      new QueryMetadataExecutor());
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

  private String[] pruneFilterTreeStatement = {
      "select * from root where vehicle1.d0.s0 > 10 or (vehicle2.d0.s1 < 10001 and vehicle1.d0.s1 = 3)",
      "select * from root where vehicle1.d0.s0 > 10 or (vehicle2.d0.s1 < 10001 and time < 1555938482600)",
      "select * from root where vehicle1.d0.s0 > 10 and not(vehicle2.d0.s1 < 10001 or vehicle1.d0.s1 = 3)"
  };

  private static final IExpression[] correntPruneFilterTree = new IExpression[6];

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

    // result of prune filter tree
    QueryPlan queryPlan = (QueryPlan) queryProcessor.parseSQLToPhysicalPlan(
        "select vehicle1.d0.s0,vehicle1.d0.s1 from root where vehicle1.d0.s0 > 10 or vehicle1.d0.s1 = 3");
    QueryExpression queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    IExpression optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    queryPlan.setExpression(optimizedExpression);
    correntPruneFilterTree[0] = optimizedExpression;

    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select vehicle1.d0.s0 from root");
    assertNull(queryPlan.getExpression());
    correntPruneFilterTree[1] = new TrueExpression();

    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select vehicle1.d0.s0 from root");
    assertNull(queryPlan.getExpression());
    correntPruneFilterTree[2] = new TrueExpression();

    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select vehicle2.d0.s1 from root");
    assertNull(queryPlan.getExpression());
    correntPruneFilterTree[3] = new TrueExpression();

    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select vehicle1.d0.s0 from root");
    assertNull(queryPlan.getExpression());
    correntPruneFilterTree[4] = new TrueExpression();

    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan("select vehicle2.d0.s1 from root");
    assertNull(queryPlan.getExpression());
    correntPruneFilterTree[5] = new TrueExpression();

  }

  @Test
  public void getAllExpressionSeries() {
  }

  @Test
  public void pruneFilterTrue() throws Exception{
    QueryPlan queryPlan;
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      initCorrectResult();
    }

    // first
    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan(pruneFilterTreeStatement[0]);
    QueryExpression queryExpression = QueryExpression.create()
        .setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    IExpression optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("root.vehicle1.d0.s0"));
    pathList.add(new Path("root.vehicle1.d0.s1"));
    IExpression pruneExpression = ExpressionUtils.pruneFilterTree(optimizedExpression.clone(), pathList);
    assertEquals(correntPruneFilterTree[0].toString(), pruneExpression.toString());

    pathList=new ArrayList<>();
    pathList.add(new Path("root.vehicle2.d0.s1"));
    pruneExpression = ExpressionUtils.pruneFilterTree(optimizedExpression.clone(), pathList);
    assertEquals(correntPruneFilterTree[1].toString(), pruneExpression.toString());

    // second
    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan(pruneFilterTreeStatement[1]);
    queryExpression = QueryExpression.create()
        .setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    pathList = new ArrayList<>();
    pathList.add(new Path("root.vehicle1.d0.s0"));
    pruneExpression = ExpressionUtils.pruneFilterTree(optimizedExpression.clone(), pathList);
    assertEquals(correntPruneFilterTree[2].toString(), pruneExpression.toString());

    pathList=new ArrayList<>();
    pathList.add(new Path("root.vehicle2.d0.s1"));
    pruneExpression = ExpressionUtils.pruneFilterTree(optimizedExpression.clone(), pathList);
    assertEquals(correntPruneFilterTree[3].toString(), pruneExpression.toString());

    // third
    queryPlan = (QueryPlan) queryProcessor
        .parseSQLToPhysicalPlan(pruneFilterTreeStatement[1]);
    queryExpression = QueryExpression.create()
        .setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
    queryPlan.setExpression(optimizedExpression);
    assertEquals(optimizedExpression.toString(), optimizedExpression.clone().toString());
    assertNotSame(optimizedExpression, optimizedExpression.clone());
    pathList = new ArrayList<>();
    pathList.add(new Path("root.vehicle1.d0.s0"));
    pathList.add(new Path("root.vehicle1.d0.s1"));
    pruneExpression = ExpressionUtils.pruneFilterTree(optimizedExpression.clone(), pathList);
    assertEquals(correntPruneFilterTree[4].toString(), pruneExpression.toString());

    pathList=new ArrayList<>();
    pathList.add(new Path("root.vehicle2.d0.s1"));
    pruneExpression = ExpressionUtils.pruneFilterTree(optimizedExpression.clone(), pathList);
    assertEquals(correntPruneFilterTree[5].toString(), pruneExpression.toString());

  }

}