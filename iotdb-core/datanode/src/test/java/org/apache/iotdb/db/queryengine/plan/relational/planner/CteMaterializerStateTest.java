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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QueryBody;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.commons.queryengine.utils.cte.CteDataStore;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@PowerMockIgnore({
  "com.sun.org.apache.xerces.*",
  "javax.xml.*",
  "org.xml.*",
  "javax.management.*",
  "javax.crypto.*",
  "sun.security.*",
  "java.time.*"
})
@RunWith(PowerMockRunner.class)
@PrepareForTest({Coordinator.class, SessionManager.class})
@SuppressStaticInitializationFor("org.apache.iotdb.db.queryengine.plan.Coordinator")
public class CteMaterializerStateTest {

  @BeforeClass
  public static void prepareEnvironment() {
    mockStatic(Coordinator.class);
    when(Coordinator.getInstance()).thenReturn(Mockito.mock(Coordinator.class));
    mockStatic(SessionManager.class);
    when(SessionManager.getInstance()).thenReturn(Mockito.mock(SessionManager.class));
  }

  @Test
  public void testMaterializedCteIsRefetchedWhenSameStatementIsExecutedAgain() {
    // PreparedStatementInfo caches the Query AST. The two contexts model two independent EXECUTE
    // commands that must not share a materialized result through that cached AST.
    final Query cteQuery = createMaterializedQuery();
    final Table cteReference = new Table(QualifiedName.of("cte1"));

    final Analysis firstAnalysis = new Analysis(cteQuery, Collections.emptyMap());
    firstAnalysis.registerNamedQuery(cteReference, cteQuery);
    final Analysis secondAnalysis = new Analysis(cteQuery, Collections.emptyMap());
    secondAnalysis.registerNamedQuery(cteReference, cteQuery);

    final CteDataStore firstResult = Mockito.mock(CteDataStore.class);
    final CteDataStore secondResult = Mockito.mock(CteDataStore.class);
    final CteMaterializer materializer = Mockito.spy(new CteMaterializer());
    Mockito.doReturn(firstResult, secondResult)
        .when(materializer)
        .fetchCteQueryResult(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    final MPPQueryContext firstContext = new MPPQueryContext(new QueryId("first"));
    materializer.materializeCTE(firstAnalysis, firstContext);
    assertSame(firstResult, firstContext.getCteDataStore(cteQuery));

    final MPPQueryContext secondContext = new MPPQueryContext(new QueryId("second"));
    materializer.materializeCTE(secondAnalysis, secondContext);
    assertSame(firstResult, firstContext.getCteDataStore(cteQuery));
    assertSame(secondResult, secondContext.getCteDataStore(cteQuery));
    Mockito.verify(materializer, Mockito.times(2))
        .fetchCteQueryResult(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testMaterializedCteIsFetchedOnceForMultipleReferencesInOneExecution() {
    final Query cteQuery = createMaterializedQuery();
    final Table firstReference = new Table(QualifiedName.of("cte1"));
    final Table secondReference = new Table(QualifiedName.of("cte1"));
    final Analysis analysis = new Analysis(cteQuery, Collections.emptyMap());
    analysis.registerNamedQuery(firstReference, cteQuery);
    analysis.registerNamedQuery(secondReference, cteQuery);

    final CteDataStore result = Mockito.mock(CteDataStore.class);
    final CteMaterializer materializer = Mockito.spy(new CteMaterializer());
    Mockito.doReturn(result)
        .when(materializer)
        .fetchCteQueryResult(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    final MPPQueryContext context = new MPPQueryContext(new QueryId("same_execution"));
    materializer.materializeCTE(analysis, context);
    // Calling the planner hook again in the same execution must also reuse the recorded result.
    materializer.materializeCTE(analysis, context);

    assertSame(result, context.getCteDataStore(firstReference));
    assertSame(result, context.getCteDataStore(secondReference));
    Mockito.verify(materializer, Mockito.times(1))
        .fetchCteQueryResult(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testInnerQuerySharesOnlyItsParentExecutionState() {
    final Query cteQuery = createMaterializedQuery();
    final Table cteReference = new Table(QualifiedName.of("cte1"));
    final CteDataStore result = Mockito.mock(CteDataStore.class);

    final MPPQueryContext parentContext = new MPPQueryContext(new QueryId("parent"));
    parentContext.recordCteMaterializationResult(cteQuery, result);
    parentContext.addCteQuery(cteReference, cteQuery);

    final MPPQueryContext innerContext = new MPPQueryContext(new QueryId("inner"));
    innerContext.setCteMaterializationContext(parentContext.getCteMaterializationContext());
    assertSame(result, innerContext.getCteDataStore(cteQuery));
    assertSame(result, innerContext.getCteDataStore(cteReference));

    final MPPQueryContext independentContext = new MPPQueryContext(new QueryId("independent"));
    assertNull(independentContext.getCteDataStore(cteQuery));
    assertNull(independentContext.getCteDataStore(cteReference));
  }

  @Test
  public void testRetryClearsTopLevelExecutionState() {
    final Query cteQuery = createMaterializedQuery();
    final Table cteReference = new Table(QualifiedName.of("cte1"));
    final MPPQueryContext context = new MPPQueryContext(new QueryId("retry"));
    context.recordCteMaterializationResult(cteQuery, Mockito.mock(CteDataStore.class));
    context.addCteQuery(cteReference, cteQuery);

    context.prepareForRetry();

    assertFalse(context.isCteMaterializationAttempted(cteQuery));
    assertNull(context.getCteDataStore(cteQuery));
    assertNull(context.getCteDataStore(cteReference));
  }

  private static Query createMaterializedQuery() {
    final Query query =
        new Query(
            Optional.empty(),
            Mockito.mock(QueryBody.class),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    query.setMaterialized(true);
    return query;
  }
}
