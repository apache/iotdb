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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllTemplateSetInfoPlan;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeLeaseRecoveryResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

public class LeaseRecoverySnapshotRegressionTest {

  @Test
  public void consensusFailureMustNotBecomeAnEmptyTemplateSnapshot() throws ConsensusException {
    final IManager manager = Mockito.mock(IManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(manager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.read(Mockito.any(GetAllTemplateSetInfoPlan.class)))
        .thenThrow(new ConsensusException("injected ConfigRegion read failure"));

    final ClusterSchemaManager schemaManager = new ClusterSchemaManager(manager, null, null);

    Assert.assertNotEquals(
        "A failed consensus read must not be represented as a valid empty snapshot",
        0,
        schemaManager.getAllTemplateSetInfo().length);
  }

  @Test
  public void emptyTemplateSnapshotMustNotReturnSuccessfulLeaseRecovery() throws Exception {
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ClusterSchemaManager schemaManager = Mockito.mock(ClusterSchemaManager.class);
    Mockito.when(configManager.confirmLeader())
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    Mockito.when(schemaManager.getAllTableInfoForDataNodeActivation()).thenReturn(new byte[] {1});
    Mockito.when(schemaManager.getAllTemplateSetInfo()).thenReturn(new byte[0]);
    Mockito.when(configManager.reloadCacheAfterLeaseRecovery()).thenCallRealMethod();

    final Field schemaManagerField = ConfigManager.class.getDeclaredField("clusterSchemaManager");
    schemaManagerField.setAccessible(true);
    schemaManagerField.set(configManager, schemaManager);

    final TDataNodeLeaseRecoveryResp response = configManager.reloadCacheAfterLeaseRecovery();

    Assert.assertNotEquals(
        "An incomplete cache snapshot must keep the DataNode fenced",
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        response.getStatus().getCode());
  }
}
