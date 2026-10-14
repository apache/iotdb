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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalExecutionPlanContextBudgetTest {
  @Test
  public void testFixedBudgetsInheritedByPipelinesAndWiredBySide() {
    long previous =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    try {
      IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(513);
      FragmentInstanceContext instance = mock(FragmentInstanceContext.class);
      when(instance.getId())
          .thenReturn(new FragmentInstanceId(new PlanFragmentId(new QueryId("budget"), 0), "0"));
      LocalExecutionPlanContext context =
          new LocalExecutionPlanContext(new TypeProvider(), instance, (DataNodeQueryContext) null);
      context.setExchangeSumNum(500);
      assertEquals(256, context.getInputMaxBytes());
      assertEquals(257, context.getOutputMaxBytes());
      IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(1024);
      LocalExecutionPlanContext child = context.createSubContext();
      assertEquals(256, child.getInputMaxBytes());
      assertEquals(257, child.getOutputMaxBytes());
      ISink sink = mock(ISink.class);
      child.setISink(sink);
      verify(sink).setMaxBytesCanReserve(257);
      ISourceHandle remote = mock(ISourceHandle.class);
      PipelineSourceHandle pipeline = mock(PipelineSourceHandle.class);
      ExchangeOperator remoteOperator = mock(ExchangeOperator.class);
      ExchangeOperator pipelineOperator = mock(ExchangeOperator.class);
      when(remoteOperator.getSourceHandle()).thenReturn(remote);
      when(pipelineOperator.getSourceHandle()).thenReturn(pipeline);
      context.addExchangeOperator(remoteOperator);
      child.addExchangeOperator(pipelineOperator);
      context.setExchangeMemoryLimits();
      verify(remote).setMaxBytesCanReserve(256);
      verify(pipeline).setMaxBytesCanReserve(257);
    } finally {
      IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(previous);
    }
  }
}
