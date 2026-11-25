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

package org.apache.iotdb.commons.consensus.index;

import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ProgressIndexTest {

  @Test
  public void testGetProgressIndexByTypeFromStateWrappedHybridProgressIndex() {
    final MetaProgressIndex metaProgressIndex = new MetaProgressIndex(10L);
    final SimpleProgressIndex simpleProgressIndex = new SimpleProgressIndex(1, 2L);
    final ProgressIndex hybridProgressIndex =
        new HybridProgressIndex(metaProgressIndex)
            .updateToMinimumEqualOrIsAfterProgressIndex(simpleProgressIndex);
    final StateProgressIndex stateProgressIndex =
        new StateProgressIndex(1L, Collections.emptyMap(), hybridProgressIndex);

    Assert.assertEquals(
        metaProgressIndex,
        stateProgressIndex.getProgressIndexByType(MetaProgressIndex.class).orElse(null));
    Assert.assertEquals(
        simpleProgressIndex,
        stateProgressIndex.getProgressIndexByType(SimpleProgressIndex.class).orElse(null));
    Assert.assertSame(
        hybridProgressIndex,
        stateProgressIndex.getProgressIndexByType(HybridProgressIndex.class).orElse(null));
    Assert.assertFalse(
        stateProgressIndex.getProgressIndexByType(TimeWindowStateProgressIndex.class).isPresent());
  }

  @Test
  public void testTimeWindowStateProgressIndexBlendsWithOtherProgressIndexTypes() {
    final TimeWindowStateProgressIndex timeWindowStateProgressIndex =
        new TimeWindowStateProgressIndex(Collections.emptyMap());
    final SimpleProgressIndex simpleProgressIndex = new SimpleProgressIndex(1, 2L);

    final ProgressIndex blendedProgressIndex =
        timeWindowStateProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
            simpleProgressIndex);
    Assert.assertTrue(blendedProgressIndex instanceof HybridProgressIndex);
    Assert.assertEquals(
        timeWindowStateProgressIndex,
        blendedProgressIndex
            .getProgressIndexByType(TimeWindowStateProgressIndex.class)
            .orElse(null));
    Assert.assertEquals(
        simpleProgressIndex,
        blendedProgressIndex.getProgressIndexByType(SimpleProgressIndex.class).orElse(null));

    final ProgressIndex reverseBlendedProgressIndex =
        simpleProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
            timeWindowStateProgressIndex);
    Assert.assertEquals(blendedProgressIndex, reverseBlendedProgressIndex);
  }

  @Test
  public void testTimeWindowStateProgressIndexPreservesNullWindowBuffersAfterRestore()
      throws IOException {
    final Map<String, Pair<Long, ByteBuffer>> timeSeries2WindowState = new HashMap<>();
    timeSeries2WindowState.put("root.sg.d.completed", new Pair<>(10L, null));
    timeSeries2WindowState.put(
        "root.sg.d.open", new Pair<>(20L, ByteBuffer.wrap(new byte[] {1, 2, 3})));
    final TimeWindowStateProgressIndex progressIndex =
        new TimeWindowStateProgressIndex(timeSeries2WindowState);

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    progressIndex.serialize(outputStream);
    final byte[] serialized = outputStream.toByteArray();

    assertRestoredWindowStates(ProgressIndexType.deserializeFrom(ByteBuffer.wrap(serialized)));
    assertRestoredWindowStates(
        ProgressIndexType.deserializeFrom(new ByteArrayInputStream(serialized)));
  }

  private void assertRestoredWindowStates(final ProgressIndex restoredProgressIndex) {
    final TimeWindowStateProgressIndex restored =
        restoredProgressIndex
            .getProgressIndexByType(TimeWindowStateProgressIndex.class)
            .orElse(null);
    Assert.assertNotNull(restored);

    final Map<String, Pair<Long, ByteBuffer>> restoredStates =
        restored.getTimeSeries2TimestampWindowBufferPairMap();
    Assert.assertEquals(2, restoredStates.size());
    Assert.assertEquals(Long.valueOf(10L), restoredStates.get("root.sg.d.completed").getLeft());
    Assert.assertNull(restoredStates.get("root.sg.d.completed").getRight());
    Assert.assertEquals(Long.valueOf(20L), restoredStates.get("root.sg.d.open").getLeft());
    Assert.assertArrayEquals(
        new byte[] {1, 2, 3}, restoredStates.get("root.sg.d.open").getRight().array());
  }
}
