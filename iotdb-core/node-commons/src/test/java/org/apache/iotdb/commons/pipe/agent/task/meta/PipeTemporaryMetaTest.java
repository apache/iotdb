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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import org.apache.iotdb.commons.pipe.resource.PipeResourceFailureType;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PipeTemporaryMetaTest {

  @Test
  public void testRecentFailuresAreRecordedAndAggregated() {
    final PipeTemporaryMetaInAgent agentMeta = new PipeTemporaryMetaInAgent("test_pipe", 1L);
    agentMeta.recordResourceFailure(PipeResourceFailureType.NETWORK_TIMEOUT);
    agentMeta.recordResourceFailure(PipeResourceFailureType.NETWORK_TIMEOUT);
    agentMeta.recordResourceFailure(PipeResourceFailureType.MEMORY_TIMEOUT);

    Assert.assertEquals(Long.valueOf(2), agentMeta.getRecentFailures().get("network_timeout"));
    Assert.assertEquals(Long.valueOf(1), agentMeta.getRecentFailures().get("memory_timeout"));

    final PipeTemporaryMetaInCoordinator coordinatorMeta = new PipeTemporaryMetaInCoordinator();
    coordinatorMeta.setRecentFailures(1, agentMeta.getRecentFailures());
    final Map<String, Long> secondNodeFailures = new HashMap<>();
    secondNodeFailures.put("network_timeout", 3L);
    coordinatorMeta.setRecentFailures(2, secondNodeFailures);

    Assert.assertEquals(
        Long.valueOf(5), coordinatorMeta.getGlobalRecentFailures().get("network_timeout"));
    Assert.assertEquals(
        Long.valueOf(1), coordinatorMeta.getGlobalRecentFailures().get("memory_timeout"));

    coordinatorMeta.setRecentFailures(1, null);
    Assert.assertEquals(
        Long.valueOf(3), coordinatorMeta.getGlobalRecentFailures().get("network_timeout"));
    Assert.assertFalse(coordinatorMeta.getGlobalRecentFailures().containsKey("memory_timeout"));
  }
}
