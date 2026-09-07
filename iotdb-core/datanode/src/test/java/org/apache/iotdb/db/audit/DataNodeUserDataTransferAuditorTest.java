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

package org.apache.iotdb.db.audit;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(StorageEngine.class)
public class DataNodeUserDataTransferAuditorTest {

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private boolean auditLogEnabled;

  @Before
  public void setUp() {
    auditLogEnabled = commonConfig.isEnableAuditLog();
  }

  @After
  public void tearDown() {
    commonConfig.setEnableAuditLog(auditLogEnabled);
  }

  @Test
  public void testAuditDatabaseIsExcludedFromGroupTransferAudit() {
    assertFalse(DataNodeUserDataTransferAuditor.containsUserData("__audit"));
    assertFalse(DataNodeUserDataTransferAuditor.containsUserData("root.__audit"));
    assertTrue(DataNodeUserDataTransferAuditor.containsUserData("root.sg"));
  }

  @Test
  public void testIoTConsensusV2AuditGateExcludesAuditDatabase() {
    final StorageEngine storageEngine = mock(StorageEngine.class);
    final DataRegion auditDataRegion = mock(DataRegion.class);
    final DataRegion userDataRegion = mock(DataRegion.class);
    final DataRegionId auditDataRegionId = new DataRegionId(1);
    final DataRegionId userDataRegionId = new DataRegionId(2);
    PowerMockito.mockStatic(StorageEngine.class);
    PowerMockito.when(StorageEngine.getInstance()).thenReturn(storageEngine);
    when(storageEngine.getDataRegion(auditDataRegionId)).thenReturn(auditDataRegion);
    when(storageEngine.getDataRegion(userDataRegionId)).thenReturn(userDataRegion);
    when(auditDataRegion.getDatabaseName()).thenReturn("root.__audit");
    when(userDataRegion.getDatabaseName()).thenReturn("root.sg");

    commonConfig.setEnableAuditLog(true);

    assertFalse(DataNodeUserDataTransferAuditor.isEnabledFor(auditDataRegionId));
    assertTrue(DataNodeUserDataTransferAuditor.isEnabledFor(userDataRegionId));
  }

  @Test
  public void testIoTConsensusV2AuditGateShortCircuitsWhenDisabled() {
    final StorageEngine storageEngine = mock(StorageEngine.class);
    PowerMockito.mockStatic(StorageEngine.class);
    PowerMockito.when(StorageEngine.getInstance()).thenReturn(storageEngine);
    commonConfig.setEnableAuditLog(false);

    assertFalse(DataNodeUserDataTransferAuditor.isEnabledFor(new DataRegionId(1)));

    verify(storageEngine, never()).getDataRegion(new DataRegionId(1));
  }

  @Test
  public void testAuditDatabaseIsExcludedFromConsensusTransferAudit() {
    final InsertNode insertNode = mock(InsertNode.class);

    assertFalse(DataNodeUserDataTransferAuditor.containsUserData("__audit", insertNode));
    assertFalse(DataNodeUserDataTransferAuditor.containsUserData("root.__audit", insertNode));
    assertTrue(DataNodeUserDataTransferAuditor.containsUserData("root.sg", insertNode));
  }

  @Test
  public void testNonInsertConsensusRequestIsExcluded() {
    final PlanNode planNode = mock(PlanNode.class);
    when(planNode.getChildren()).thenReturn(Collections.emptyList());

    assertFalse(DataNodeUserDataTransferAuditor.containsUserData("root.sg", planNode));
  }

  @Test
  public void testClassificationDoesNotDeserializeConsensusRequest() {
    final IConsensusRequest request = mock(IConsensusRequest.class);

    assertFalse(DataNodeUserDataTransferAuditor.containsUserData("root.sg", request));
    verify(request, never()).serializeToByteBuffer();
  }

  @Test
  public void testObjectFileNodeContainsUserData() {
    assertTrue(DataNodeUserDataTransferAuditor.containsUserData(mock(ObjectNode.class)));
  }
}
