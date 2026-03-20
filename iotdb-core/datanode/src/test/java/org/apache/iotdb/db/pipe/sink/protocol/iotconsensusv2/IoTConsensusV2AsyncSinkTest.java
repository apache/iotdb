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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
import org.apache.iotdb.db.pipe.consensus.metric.IoTConsensusV2SinkMetrics;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class IoTConsensusV2AsyncSinkTest {

  @Test
  public void testLeaderReplicateProgressIgnoresPreAssignedButDiscardedEvents() throws Exception {
    IoTConsensusV2AsyncSink sink = new IoTConsensusV2AsyncSink();
    setField(sink, "iotConsensusV2SinkMetrics", Mockito.mock(IoTConsensusV2SinkMetrics.class));

    String pipeName = "__consensus.test_" + System.nanoTime();
    try {
      Assert.assertEquals(
          1L, ReplicateProgressDataNodeManager.assignReplicateIndexForIoTV2(pipeName));
      Assert.assertEquals(
          "Source-side replicate index assignment should not affect connector sync lag progress",
          0L,
          sink.getLeaderReplicateProgress());

      EnrichedEvent enqueuedEvent = Mockito.mock(EnrichedEvent.class);
      Mockito.when(enqueuedEvent.getReplicateIndexForIoTV2()).thenReturn(1L);
      Mockito.when(enqueuedEvent.increaseReferenceCount(Mockito.anyString())).thenReturn(true);

      Assert.assertTrue(invokeAddEvent2Buffer(sink, enqueuedEvent));
      Assert.assertEquals(1L, sink.getLeaderReplicateProgress());

      Assert.assertEquals(
          2L, ReplicateProgressDataNodeManager.assignReplicateIndexForIoTV2(pipeName));
      Assert.assertEquals(
          "Discarded events that never enter the connector must not create phantom lag",
          1L,
          sink.getLeaderReplicateProgress());
    } finally {
      ReplicateProgressDataNodeManager.resetReplicateIndexForIoTV2(pipeName);
    }
  }

  private boolean invokeAddEvent2Buffer(IoTConsensusV2AsyncSink sink, EnrichedEvent event)
      throws Exception {
    Method method =
        IoTConsensusV2AsyncSink.class.getDeclaredMethod("addEvent2Buffer", EnrichedEvent.class);
    method.setAccessible(true);
    return (boolean) method.invoke(sink, event);
  }

  private void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
