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

package org.apache.iotdb.confignode.manager.pipe.coordinator.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.task.PipeTaskCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;

public class PipeMetaSyncerTest {

  @Test
  public void testConsensusPipeCountMismatchTriggersCheckImmediately() throws Exception {
    // A count mismatch means a region lost its replication channel, so the check runs on this round
    // even though the periodic interval has not elapsed, and keeps running until the counts match.
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ProcedureManager procedureManager = Mockito.mock(ProcedureManager.class);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final RegionMaintainHandler regionMaintainHandler = Mockito.mock(RegionMaintainHandler.class);
    final PipeManager pipeManager = Mockito.mock(PipeManager.class);
    final PipeTaskCoordinator pipeTaskCoordinator = Mockito.mock(PipeTaskCoordinator.class);
    Mockito.when(configManager.getProcedureManager()).thenReturn(procedureManager);
    Mockito.when(configManager.getPipeManager()).thenReturn(pipeManager);
    Mockito.when(pipeManager.getPipeTaskCoordinator()).thenReturn(pipeTaskCoordinator);
    Mockito.when(procedureManager.getEnv()).thenReturn(env);
    Mockito.when(env.getRegionMaintainHandler()).thenReturn(regionMaintainHandler);
    // A failure status keeps the syncer from skipping the following rounds.
    Mockito.when(procedureManager.pipeMetaSync())
        .thenReturn(new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()));
    Mockito.when(regionMaintainHandler.hasConsensusPipeCountMismatch()).thenReturn(true);

    final PipeMetaSyncer pipeMetaSyncer = new PipeMetaSyncer(configManager);

    sync(pipeMetaSyncer);
    Mockito.verify(regionMaintainHandler, Mockito.times(1)).checkAndRepairConsensusPipes();

    sync(pipeMetaSyncer);
    Mockito.verify(regionMaintainHandler, Mockito.times(2)).checkAndRepairConsensusPipes();

    // The counts match again: the next 4 rounds stay below the interval and must not check.
    Mockito.when(regionMaintainHandler.hasConsensusPipeCountMismatch()).thenReturn(false);
    for (int i = 0; i < 4; ++i) {
      sync(pipeMetaSyncer);
    }
    Mockito.verify(regionMaintainHandler, Mockito.times(2)).checkAndRepairConsensusPipes();
  }

  private static void sync(final PipeMetaSyncer pipeMetaSyncer) throws Exception {
    final Method sync = PipeMetaSyncer.class.getDeclaredMethod("sync");
    sync.setAccessible(true);
    sync.invoke(pipeMetaSyncer);
  }
}
