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

package org.apache.iotdb.db.queryengine.plan;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorTest {

  private static final long QUERY_ID = 10_000L;

  private static int previousDataNodeId;
  private static ExecutorService stateMachineExecutor;

  @BeforeClass
  public static void setUp() throws Exception {
    // the coordinator builds its query id generator from the data node id of this node
    previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    UDFClassLoaderManager.setupAndGetInstance();
    stateMachineExecutor = Executors.newSingleThreadExecutor();
  }

  @AfterClass
  public static void tearDown() {
    stateMachineExecutor.shutdownNow();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
  }

  @Test
  public void testRejectedReadOnlyInternalQueryClosesUDFQueryScope() throws Exception {
    // Only the analysis tells that an IoTDBLocal statement is not a query. By then it may have
    // pinned the class loaders of the UDFs it uses in the query scope of the execution.
    IPlanner planner = mock(IPlanner.class);
    when(planner.analyze(any(MPPQueryContext.class)))
        .thenAnswer(
            invocation -> {
              MPPQueryContext queryContext = invocation.getArgument(0);
              queryContext.setQueryType(QueryType.WRITE);
              return mock(IAnalysis.class);
            });
    QueryExecution[] createdExecution = new QueryExecution[1];

    SemanticException thrown =
        assertThrows(
            SemanticException.class,
            () ->
                executeReadOnlyInternalQuery(
                    (queryContext, startTime) -> {
                      createdExecution[0] =
                          new QueryExecution(planner, queryContext, stateMachineExecutor);
                      return createdExecution[0];
                    }));

    assertEquals(
        DataNodeQueryMessages
            .EXCEPTION_ONLY_QUERY_IS_SUPPORTED_FOR_IOTDBLOCAL_QUERY_INTERFACE_09195570,
        thrown.getMessage());
    // the rejected execution is never registered, so no later cleanup can find it
    assertNull(Coordinator.getInstance().getQueryExecution(QUERY_ID));
    assertUDFQueryScopeClosed(createdExecution[0]);
  }

  private static void executeReadOnlyInternalQuery(
      BiFunction<MPPQueryContext, Long, IQueryExecution> queryExecutionFactory) throws Throwable {
    Method execution =
        Coordinator.class.getDeclaredMethod(
            "execution",
            long.class,
            SessionInfo.class,
            String.class,
            boolean.class,
            boolean.class,
            boolean.class,
            BiFunction.class);
    execution.setAccessible(true);
    try {
      execution.invoke(
          Coordinator.getInstance(),
          QUERY_ID,
          new SessionInfo(0, "test", ZoneId.systemDefault()),
          "INSERT INTO t2(time, s) SELECT time, f(s) FROM t1",
          false,
          false,
          true,
          queryExecutionFactory);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private static void assertUDFQueryScopeClosed(QueryExecution execution) throws Exception {
    assertNotNull(execution);
    Field queryScopeField = QueryExecution.class.getDeclaredField("udfQueryScope");
    queryScopeField.setAccessible(true);
    UDFClassLoaderManager.QueryScope queryScope =
        (UDFClassLoaderManager.QueryScope) queryScopeField.get(execution);
    assertNotNull(queryScope);
    assertThrows(
        IllegalStateException.class,
        () -> UDFClassLoaderManager.getInstance().withQueryScope(queryScope, () -> null));
  }
}
