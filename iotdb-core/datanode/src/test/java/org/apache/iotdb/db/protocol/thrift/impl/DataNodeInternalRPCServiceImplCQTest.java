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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.IAuthorityFetcher;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.metrics.metricsets.system.SystemMetrics;
import org.apache.iotdb.mpp.rpc.thrift.TExecuteCQ;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.util.MemoizedSupplier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class DataNodeInternalRPCServiceImplCQTest {

  private static final String USERNAME = "cq_unit_user";
  private static int originalDataNodeId;

  private MemoizedSupplier<IAuthorityFetcher> authorityFetcherSupplier;
  private IAuthorityFetcher originalAuthorityFetcher;
  private IAuthorityFetcher authorityFetcher;
  private ISchemaFetcher schemaFetcher;
  private DataNodeInternalRPCServiceImpl service;
  private int originalQueryCount;

  @BeforeClass
  public static void setUpClass() {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @AfterClass
  public static void tearDownClass() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  @Before
  public void setUp() throws Exception {
    // AuthorityChecker has no access-control hook on this branch, so replace its memoized
    // authority fetcher for the duration of each test.
    authorityFetcherSupplier =
        Whitebox.getInternalState(AuthorityChecker.class, "authorityFetcher");
    originalAuthorityFetcher = authorityFetcherSupplier.get();
    authorityFetcher = mock(IAuthorityFetcher.class);
    Whitebox.setInternalState(authorityFetcherSupplier, "value", authorityFetcher);

    service = new DataNodeInternalRPCServiceImpl(mock(SystemMetrics.class));
    schemaFetcher = mock(ISchemaFetcher.class);
    Field field = DataNodeInternalRPCServiceImpl.class.getDeclaredField("schemaFetcher");
    field.setAccessible(true);
    field.set(service, schemaFetcher);
    originalQueryCount = Coordinator.getInstance().getQueryExecutionMapSize();
  }

  @After
  public void tearDown() {
    Whitebox.setInternalState(authorityFetcherSupplier, "value", originalAuthorityFetcher);
  }

  @Test
  public void testAuthorizationFailureStopsBeforeQueryAnalysis() throws Exception {
    when(authorityFetcher.getAuthorizedPatternTree(USERNAME, PrivilegeType.READ_DATA.ordinal()))
        .thenThrow(new AuthException(TSStatusCode.NO_PERMISSION, "read denied"));

    TSStatus result = service.executeCQ(request());
    assertEquals(result.toString(), TSStatusCode.NO_PERMISSION.getStatusCode(), result.getCode());
    assertEquals("read denied", result.getMessage());
    assertOwnerIdentity();
    verifyZeroInteractions(schemaFetcher);
    assertSessionAndQueryCleanedUp();
  }

  @Test
  public void testAuthorizedExecutionPropagatesOwnerIdentity() throws Exception {
    PathPatternTree authorizedScope = new PathPatternTree();
    authorizedScope.appendPathPattern(new PartialPath("root.cq_unit.source.**"));
    authorizedScope.constructTree();
    when(authorityFetcher.getAuthorizedPatternTree(USERNAME, PrivilegeType.READ_DATA.ordinal()))
        .thenReturn(authorizedScope);
    when(schemaFetcher.fetchSchema(
            any(PathPatternTree.class), anyBoolean(), any(MPPQueryContext.class)))
        .thenReturn(new ClusterSchemaTree());

    TSStatus result = service.executeCQ(request());
    assertEquals(result.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getCode());
    assertOwnerIdentity();
    ArgumentCaptor<MPPQueryContext> context = ArgumentCaptor.forClass(MPPQueryContext.class);
    verify(schemaFetcher).fetchSchema(any(PathPatternTree.class), anyBoolean(), context.capture());
    assertEquals(USERNAME, context.getValue().getSession().getUserName());
    assertSessionAndQueryCleanedUp();
  }

  private void assertOwnerIdentity() throws Exception {
    verify(authorityFetcher).getAuthorizedPatternTree(USERNAME, PrivilegeType.READ_DATA.ordinal());
  }

  private void assertSessionAndQueryCleanedUp() {
    assertNull(SessionManager.getInstance().getCurrSession());
    assertEquals(originalQueryCount, Coordinator.getInstance().getQueryExecutionMapSize());
  }

  private TExecuteCQ request() {
    return new TExecuteCQ(
        "SELECT s1 INTO root.cq_unit.target(::) FROM root.cq_unit.source",
        0,
        1000,
        10000,
        "UTC",
        "cq_unit",
        USERNAME);
  }
}
