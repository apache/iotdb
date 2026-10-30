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

package org.apache.iotdb.confignode.consensus.request;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProceduresPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.testonly.NeverFinishProcedure;
import org.apache.iotdb.confignode.procedure.store.ConfigProcedureStore;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdateProceduresPlanTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void batchRoundTripAndSnapshotRetainAllProcedures() throws Exception {
    List<Procedure<ConfigNodeProcedureEnv>> procedures =
        Arrays.asList(
            new NeverFinishProcedure(7), new NeverFinishProcedure(8), new NeverFinishProcedure(4));
    UpdateProceduresPlan batch = new UpdateProceduresPlan(procedures);
    UpdateProceduresPlan restored =
        (UpdateProceduresPlan) ConfigPhysicalPlan.Factory.create(batch.serializeToByteBuffer());
    assertEquals(batch, restored);
    ProcedureInfo info = new ProcedureInfo(null);
    info.updateProcedures(restored);
    File snapshot = folder.newFolder();
    assertTrue(info.processTakeSnapshot(snapshot));
    ProcedureInfo recovered = new ProcedureInfo(null);
    recovered.processLoadSnapshot(snapshot);
    assertEquals(info, recovered);
    assertEquals(3, recovered.getProcedures().size());
    assertEquals(9, recovered.getNextProcId());
    // Existing single-procedure log entries retain their format and reader.
    UpdateProcedurePlan single = new UpdateProcedurePlan(procedures.get(0));
    assertEquals(single, ConfigPhysicalPlan.Factory.create(single.serializeToByteBuffer()));
  }

  @Test
  public void storeWritesOneConsensusEntryForWholeBatch() throws Exception {
    ConfigManager manager = mock(ConfigManager.class);
    ConsensusManager consensus = mock(ConsensusManager.class);
    when(manager.getConsensusManager()).thenReturn(consensus);
    when(consensus.write(any()))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    ConfigProcedureStore store = new ConfigProcedureStore(manager, new ProcedureInfo(null));
    Procedure[] procedures = {
      new NeverFinishProcedure(7), new NeverFinishProcedure(8), new NeverFinishProcedure(4)
    };
    store.update(procedures);
    ArgumentCaptor<ConfigPhysicalPlan> plan = ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    verify(consensus).write(plan.capture());
    assertEquals(
        Arrays.asList(procedures), ((UpdateProceduresPlan) plan.getValue()).getProcedures());
  }

  @Test
  public void rejectedBatchIsNotReportedAsPersisted() throws Exception {
    ConfigManager manager = mock(ConfigManager.class);
    ConsensusManager consensus = mock(ConsensusManager.class);
    when(manager.getConsensusManager()).thenReturn(consensus);
    when(consensus.write(any()))
        .thenReturn(new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()));
    ConfigProcedureStore store = new ConfigProcedureStore(manager, new ProcedureInfo(null));
    try {
      store.update(new Procedure[] {new NeverFinishProcedure(7)});
      fail("Rejected consensus write must be retried by the executor");
    } catch (ConsensusException expected) {
      assertTrue(
          expected
              .getMessage()
              .contains(String.valueOf(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())));
    }
  }
}
