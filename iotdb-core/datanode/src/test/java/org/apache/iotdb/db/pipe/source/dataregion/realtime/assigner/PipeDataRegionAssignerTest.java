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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner;

import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeDataRegionAssigner.CompletionToken;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PipeDataRegionAssignerTest {

  @Test
  public void testNullDataEventMarksPublicationFailed() {
    try (final PipeDataRegionAssigner assigner = new PipeDataRegionAssigner(Integer.MIN_VALUE)) {
      final long generation = assigner.getPublishedDataGeneration();
      final long failureEpoch = assigner.getPublicationFailureEpoch();

      assigner.publishDataEventToAssign(() -> null);

      assertEquals(generation + 1, assigner.getPublishedDataGeneration());
      assertEquals(failureEpoch + 1, assigner.getPublicationFailureEpoch());
    }
  }

  @Test
  public void testOnlyLatestCompletionInvalidationCanPublishBarrier() {
    try (final PipeDataRegionAssigner assigner = new PipeDataRegionAssigner(Integer.MIN_VALUE)) {
      final CompletionToken staleToken = assigner.invalidateCompletionAndGetToken();
      final CompletionToken currentToken = assigner.invalidateCompletionAndGetToken();

      assertFalse(assigner.publishCompletionBarrier(staleToken));
      assigner.invalidateCompletion();
      assertTrue(assigner.publishCompletionBarrier(currentToken));
    }
  }

  @Test
  public void testInsertInvalidationRejectsCurrentCompletionToken() {
    try (final PipeDataRegionAssigner assigner = new PipeDataRegionAssigner(Integer.MIN_VALUE)) {
      final long generation = assigner.getPublishedDataGeneration();
      final CompletionToken ignoredInsertToken = assigner.invalidateCompletionAndGetToken();

      assigner.invalidateCompletionBarrier();

      assertEquals(generation + 2, assigner.getPublishedDataGeneration());
      assertFalse(assigner.publishCompletionBarrier(ignoredInsertToken));

      final CompletionToken publishedInsertToken = assigner.invalidateCompletionAndGetToken();
      assigner.publishInsertDataEventToAssign(
          () -> PipeRealtimeEventFactory.createRealtimeEvent(Integer.MIN_VALUE, false));
      assertFalse(assigner.publishCompletionBarrier(publishedInsertToken));
    }
  }
}
