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

package org.apache.iotdb.db.pipe.agent.task.execution;

import org.apache.iotdb.db.pipe.consensus.IoTConsensusV2SubtaskExecutor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class PipeSubtaskExecutorManagerTest {

  @After
  public void tearDown() {
    // Recreate the shared consensus executor so the following tests are not affected.
    PipeSubtaskExecutorManager.getInstance().getConsensusExecutor();
  }

  /** The consensus executor is a singleton while it is alive. */
  @Test
  public void testGetConsensusExecutor() {
    final PipeSubtaskExecutorManager manager = PipeSubtaskExecutorManager.getInstance();

    final IoTConsensusV2SubtaskExecutor consensusExecutor = manager.getConsensusExecutor();
    Assert.assertFalse(consensusExecutor.isShutdown());
    Assert.assertSame(consensusExecutor, manager.getConsensusExecutor());

    consensusExecutor.shutdown();
    Assert.assertTrue(consensusExecutor.isShutdown());
  }

  /**
   * A shut-down consensus executor is recreated so that consensus pipes can start their subtasks.
   */
  @Test
  public void testGetConsensusExecutorRecoversFromShutdown() {
    final PipeSubtaskExecutorManager manager = PipeSubtaskExecutorManager.getInstance();

    final IoTConsensusV2SubtaskExecutor consensusExecutor = manager.getConsensusExecutor();
    consensusExecutor.shutdown();
    Assert.assertTrue(consensusExecutor.isShutdown());

    final IoTConsensusV2SubtaskExecutor recoveredExecutor = manager.getConsensusExecutor();
    Assert.assertNotSame(consensusExecutor, recoveredExecutor);
    Assert.assertFalse(recoveredExecutor.isShutdown());

    recoveredExecutor.shutdown();
  }
}
