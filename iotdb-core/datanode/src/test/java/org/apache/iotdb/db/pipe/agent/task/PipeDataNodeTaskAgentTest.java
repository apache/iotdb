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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class PipeDataNodeTaskAgentTest {

  @Test
  public void testCreateMemoryCheckStillRunsWhenNoPipeTasksNeedToBeCreated() throws Exception {
    final boolean originalPipeEnableMemoryCheck =
        CommonDescriptor.getInstance().getConfig().isPipeEnableMemoryChecked();
    final long originalPipeInsertNodeQueueMemory =
        CommonDescriptor.getInstance().getConfig().getPipeInsertNodeQueueMemory();
    final double originalPipeTotalFloatingMemoryProportion =
        CommonDescriptor.getInstance().getConfig().getPipeTotalFloatingMemoryProportion();

    try {
      CommonDescriptor.getInstance().getConfig().setIsPipeEnableMemoryChecked(true);
      CommonDescriptor.getInstance().getConfig().setPipeInsertNodeQueueMemory(1);
      CommonDescriptor.getInstance().getConfig().setPipeTotalFloatingMemoryProportion(0);

      Assert.assertThrows(
          PipeException.class,
          () ->
              PipeDataNodeAgent.task()
                  .calculateMemoryUsage(
                      new PipeMeta(
                          new PipeStaticMeta(
                              "p", 1L, new HashMap<>(), new HashMap<>(), new HashMap<>()),
                          new PipeRuntimeMeta())));
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setIsPipeEnableMemoryChecked(originalPipeEnableMemoryCheck);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeInsertNodeQueueMemory(originalPipeInsertNodeQueueMemory);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeTotalFloatingMemoryProportion(originalPipeTotalFloatingMemoryProportion);
    }
  }
}
