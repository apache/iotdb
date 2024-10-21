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

package org.apache.iotdb.db.queryengine.transformation.builder;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession.SqlDialect;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TransformOperator;
import org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.PipelineDriverFactory;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.multi.UDFQueryRowWindowTransformer;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.fail;

public class EvaluationDAGBuilderTest {

  @Test
  public void testBuildWithNonMappable() {
    String sql =
        "select s1 + 1, s1 * 2, s1 - 2, s1 / 3, sin(s1), m4(s1,'windowSize'='10') from root.sg.d1;";
    try {
      IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
      Operator operator = generateOperatorTree(sql);
      Assert.assertNotNull(operator);
      TransformOperator transformOperator =
          (TransformOperator) ((IdentitySinkOperator) operator).getChildren().get(0);
      LayerReader[] transformers = transformOperator.getTransformers();
      Assert.assertEquals(6, transformers.length);
      Assert.assertTrue(transformers[0] instanceof UDFQueryRowWindowTransformer);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private Operator generateOperatorTree(String sql) {
    try {
      UDFClassLoaderManager.setupAndGetInstance();
      Statement statement =
          StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
      QueryId queryId = new QueryId("test");
      SessionInfo sessionInfo =
          new SessionInfo(0, "root", ZoneId.systemDefault(), "root.db", SqlDialect.TREE);
      MPPQueryContext context =
          new MPPQueryContext(
              sql,
              queryId,
              sessionInfo,
              new TEndPoint("127.0.0.1", 6667),
              new TEndPoint("127.0.0.1", 6667));
      Analyzer analyzer =
          new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
      Analysis analysis = analyzer.analyze(statement);
      LogicalPlanner logicalPlanner = new LogicalPlanner(context);
      LogicalQueryPlan logicalPlan = logicalPlanner.plan(analysis);
      DistributionPlanner distributionPlanner = new DistributionPlanner(analysis, logicalPlan);
      FragmentInstance instance = distributionPlanner.planFragments().getInstances().get(0);
      LocalExecutionPlanner localExecutionPlanner = LocalExecutionPlanner.getInstance();
      List<PipelineDriverFactory> driverFactories =
          localExecutionPlanner.plan(
              instance.getFragment().getPlanNodeTree(),
              instance.getFragment().getTypeProvider(),
              mockFIContext(queryId),
              new DataNodeQueryContext(1));
      return driverFactories.get(0).getOperation();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    return null;
  }

  private FragmentInstanceContext mockFIContext(QueryId queryId) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext instanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    IDataRegionForQuery dataRegionForQuery = Mockito.mock(DataRegion.class);
    instanceContext.setDataRegion(dataRegionForQuery);
    return instanceContext;
  }
}
