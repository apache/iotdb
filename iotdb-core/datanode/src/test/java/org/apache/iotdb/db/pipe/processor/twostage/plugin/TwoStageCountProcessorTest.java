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

package org.apache.iotdb.db.pipe.processor.twostage.plugin;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TwoStageCountProcessorTest {

  @Test
  public void testOutputSeriesSupportsNewAndLegacyKeys() throws Exception {
    Assert.assertEquals(
        "root.db.d.s1", parseOutputSeries("processor.output.series", "root.db.d.s1").getFullPath());
    Assert.assertEquals(
        "root.db.d.s2", parseOutputSeries("processor.output-series", "root.db.d.s2").getFullPath());
  }

  @Test
  public void testValidateOutputSeriesSupportsNewAndLegacyKeys() throws Exception {
    validateOutputSeries("processor.output.series", "root.db.d.s1");
    validateOutputSeries("processor.output-series", "root.db.d.s2");
  }

  @Test
  public void testInitializeStateProgressIndexFromHybridProgressIndex() {
    final MetaProgressIndex metaProgressIndex = new MetaProgressIndex(10L);
    final SimpleProgressIndex simpleProgressIndex = new SimpleProgressIndex(1, 2L);
    final ProgressIndex hybridProgressIndex =
        new HybridProgressIndex(metaProgressIndex)
            .updateToMinimumEqualOrIsAfterProgressIndex(simpleProgressIndex);
    final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(hybridProgressIndex, 0);

    final StateProgressIndex stateProgressIndex =
        TwoStageCountProcessor.initializeStateProgressIndex(pipeTaskMeta);

    Assert.assertSame(stateProgressIndex, pipeTaskMeta.getProgressIndex());
    Assert.assertEquals(
        metaProgressIndex,
        stateProgressIndex.getProgressIndexByType(MetaProgressIndex.class).orElse(null));
    Assert.assertEquals(
        simpleProgressIndex,
        stateProgressIndex.getProgressIndexByType(SimpleProgressIndex.class).orElse(null));
  }

  @Test
  public void testInitializeStateProgressIndexFromTimeWindowStateProgressIndex() {
    final TimeWindowStateProgressIndex timeWindowStateProgressIndex =
        new TimeWindowStateProgressIndex(Collections.emptyMap());
    final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(timeWindowStateProgressIndex, 0);

    final StateProgressIndex stateProgressIndex =
        TwoStageCountProcessor.initializeStateProgressIndex(pipeTaskMeta);
    Assert.assertEquals(
        timeWindowStateProgressIndex,
        stateProgressIndex.getProgressIndexByType(TimeWindowStateProgressIndex.class).orElse(null));

    final SimpleProgressIndex simpleProgressIndex = new SimpleProgressIndex(1, 2L);
    final ProgressIndex updatedProgressIndex =
        pipeTaskMeta.updateProgressIndex(
            new StateProgressIndex(1L, Collections.emptyMap(), simpleProgressIndex));
    Assert.assertTrue(updatedProgressIndex instanceof StateProgressIndex);
    Assert.assertEquals(
        timeWindowStateProgressIndex,
        updatedProgressIndex
            .getProgressIndexByType(TimeWindowStateProgressIndex.class)
            .orElse(null));
    Assert.assertEquals(
        simpleProgressIndex,
        updatedProgressIndex.getProgressIndexByType(SimpleProgressIndex.class).orElse(null));
  }

  private PartialPath parseOutputSeries(final String key, final String value) throws Exception {
    return TwoStageCountProcessor.parseOutputSeries(
        new PipeParameters(Collections.singletonMap(key, value)));
  }

  private void validateOutputSeries(final String key, final String value) throws Exception {
    new TwoStageCountProcessor()
        .validate(
            new PipeParameterValidator(new PipeParameters(Collections.singletonMap(key, value))));
  }
}
