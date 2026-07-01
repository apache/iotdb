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

package org.apache.iotdb.confignode.manager.pipe.coordinator.task;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PipeTaskCoordinatorTest {

  @Test
  public void testLegacyStartPipeRequestResolvesTableOnlyPipe() {
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ProcedureManager procedureManager = Mockito.mock(ProcedureManager.class);
    final PipeTaskInfo pipeTaskInfo = Mockito.mock(PipeTaskInfo.class);
    final PipeTaskCoordinator pipeTaskCoordinator =
        new PipeTaskCoordinator(configManager, pipeTaskInfo);

    Mockito.when(configManager.getProcedureManager()).thenReturn(procedureManager);
    Mockito.when(pipeTaskInfo.isPipeExisted("p1", false)).thenReturn(false);
    Mockito.when(pipeTaskInfo.isPipeExisted("p1", true)).thenReturn(true);
    Mockito.when(procedureManager.startPipe("p1", true))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    final TSStatus status = pipeTaskCoordinator.startPipe(new TStartPipeReq("p1"));

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Mockito.verify(pipeTaskInfo).isPipeExisted("p1", false);
    Mockito.verify(pipeTaskInfo, Mockito.times(2)).isPipeExisted("p1", true);
    Mockito.verify(pipeTaskInfo, Mockito.never()).isPipeExisted("p1");
    Mockito.verify(procedureManager).startPipe("p1", true);
    Mockito.verify(procedureManager, Mockito.never()).startPipe("p1");
  }

  @Test
  public void testExplicitStartPipeRequestUsesModelScopedPipeExistenceCheck() {
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final PipeTaskInfo pipeTaskInfo = Mockito.mock(PipeTaskInfo.class);
    final PipeTaskCoordinator pipeTaskCoordinator =
        new PipeTaskCoordinator(configManager, pipeTaskInfo);

    Mockito.when(pipeTaskInfo.isPipeExisted("p1", true)).thenReturn(false);

    final TSStatus status =
        pipeTaskCoordinator.startPipe(new TStartPipeReq("p1").setIsTableModel(true));

    Assert.assertEquals(TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(), status.getCode());
    Mockito.verify(pipeTaskInfo).isPipeExisted("p1", true);
    Mockito.verify(pipeTaskInfo, Mockito.never()).isPipeExisted("p1");
    Mockito.verify(configManager, Mockito.never()).getProcedureManager();
  }
}
