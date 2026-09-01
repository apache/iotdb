/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.source.dataregion.realtime.listener;

import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeDataRegionAssigner;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipeInsertionDataNodeListenerTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testInsertUsesBarrierInvalidatingPublicationPath() throws Exception {
    final PipeInsertionDataNodeListener listener = PipeInsertionDataNodeListener.getInstance();
    final Field assignerMapField =
        PipeInsertionDataNodeListener.class.getDeclaredField("dataRegionId2Assigner");
    assignerMapField.setAccessible(true);
    final ConcurrentMap<Integer, PipeDataRegionAssigner> assignerMap =
        (ConcurrentMap<Integer, PipeDataRegionAssigner>) assignerMapField.get(listener);
    final int dataRegionId = Integer.MIN_VALUE + 1;
    final PipeDataRegionAssigner assigner = mock(PipeDataRegionAssigner.class);
    assignerMap.put(dataRegionId, assigner);

    try {
      when(assigner.shouldListenToInsertNode()).thenReturn(false);
      listener.listenToInsertNode(dataRegionId, null, null, null);
      verify(assigner).invalidateCompletionBarrier();
      verify(assigner, never()).invalidateCompletion();

      reset(assigner);
      when(assigner.shouldListenToInsertNode()).thenReturn(true);
      listener.listenToInsertNode(dataRegionId, null, null, null);
      verify(assigner).publishInsertDataEventToAssign(any());
      verify(assigner, never()).publishDataEventToAssign(any());
    } finally {
      assignerMap.remove(dataRegionId, assigner);
    }
  }
}
