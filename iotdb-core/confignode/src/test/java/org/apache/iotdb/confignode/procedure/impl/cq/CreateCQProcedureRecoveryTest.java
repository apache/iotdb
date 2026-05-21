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

package org.apache.iotdb.confignode.procedure.impl.cq;

import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.read.cq.ShowCQPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.cq.CQManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.cq.CQInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class CreateCQProcedureRecoveryTest {

  private TCreateCQReq newCreateCQReq() {
    return new TCreateCQReq(
        "testCq1",
        1000,
        0,
        1000,
        0,
        (byte) 0,
        "select s1 into root.backup.d1.s1 from root.sg.d1",
        "create cq testCq1 BEGIN select s1 into root.backup.d1.s1 from root.sg.d1 END",
        "Asia",
        "root");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void recoverScheduledTaskShouldResubmitFromLatestMetadata() throws Exception {
    ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
    Mockito.when(
            executor.schedule(
                Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class)))
        .thenReturn(Mockito.mock(ScheduledFuture.class));

    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    CQManager cqManager = Mockito.mock(CQManager.class);
    ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(configManager.getCQManager()).thenReturn(cqManager);
    Mockito.when(cqManager.markCQLocallyScheduled(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(true);

    TCreateCQReq req = newCreateCQReq();
    CreateCQProcedure procedure = new CreateCQProcedure(req, executor);

    CQInfo cqInfo = new CQInfo();
    cqInfo.addCQ(new AddCQPlan(req, procedure.getMd5(), System.currentTimeMillis() + 10_000));
    Mockito.when(consensusManager.read(Mockito.any(ShowCQPlan.class))).thenReturn(cqInfo.showCQ());

    procedure.recoverScheduledTask(env);

    Mockito.verify(executor)
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void recoverScheduledTaskShouldSkipDuplicatedLocalSchedule() throws Exception {
    ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    CQManager cqManager = Mockito.mock(CQManager.class);
    ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(configManager.getCQManager()).thenReturn(cqManager);
    Mockito.when(cqManager.markCQLocallyScheduled(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(false);

    TCreateCQReq req = newCreateCQReq();
    CreateCQProcedure procedure = new CreateCQProcedure(req, executor);

    CQInfo cqInfo = new CQInfo();
    cqInfo.addCQ(new AddCQPlan(req, procedure.getMd5(), System.currentTimeMillis() + 10_000));
    Mockito.when(consensusManager.read(Mockito.any(ShowCQPlan.class))).thenReturn(cqInfo.showCQ());

    procedure.recoverScheduledTask(env);

    Mockito.verify(executor, Mockito.never())
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
  }
}
