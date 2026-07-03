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

package org.apache.iotdb.db.pipe.agent.task.builder;

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PipeDataNodeTaskBuilderTest {

  @Test
  public void testBlendUserAndSystemParametersDoesNotMutateOriginal() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, "root.sg.**");
    final PipeParameters userParameters = new PipeParameters(attributes);

    final PipeParameters blendedParameters =
        PipeDataNodeTaskBuilder.blendUserAndSystemParameters(
            userParameters, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));

    Assert.assertEquals(
        "root.sg.**", blendedParameters.getStringByKeys(PipeSourceConstant.EXTRACTOR_PATTERN_KEY));
    Assert.assertFalse(blendedParameters.hasAttribute(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY));
    Assert.assertFalse(userParameters.hasAttribute(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY));
  }

  @Test
  public void testBlendUserAndSystemParametersMarksRestartOrNewlyAddedTask() {
    final PipeParameters restartedParameters =
        PipeDataNodeTaskBuilder.blendUserAndSystemParameters(
            new PipeParameters(new HashMap<>()),
            new PipeTaskMeta(new SimpleProgressIndex(1, 2L), 1));

    Assert.assertEquals(
        Boolean.TRUE.toString(),
        restartedParameters.getStringByKeys(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY));

    final PipeTaskMeta newlyAddedTaskMeta =
        new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1).markAsNewlyAdded();
    final PipeParameters newlyAddedParameters =
        PipeDataNodeTaskBuilder.blendUserAndSystemParameters(
            new PipeParameters(new HashMap<>()), newlyAddedTaskMeta);

    Assert.assertEquals(
        Boolean.TRUE.toString(),
        newlyAddedParameters.getStringByKeys(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY));
  }

  @Test
  public void testPreprocessParametersInjectsRuntimeDefaults() {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(PipeSourceConstant.EXTRACTOR_INCLUSION_KEY, "data.delete");
    final PipeParameters sourceParameters = new PipeParameters(sourceAttributes);
    final PipeParameters sinkParameters = new PipeParameters(new HashMap<>());

    PipeDataNodeTaskBuilder.preprocessParameters(sourceParameters, sinkParameters);

    Assert.assertEquals(
        Boolean.FALSE.toString(),
        sinkParameters.getStringByKeys(PipeSinkConstant.CONNECTOR_REALTIME_FIRST_KEY));
    Assert.assertEquals(
        Boolean.TRUE.toString(),
        sinkParameters.getStringByKeys(PipeSinkConstant.SINK_ENABLE_SEND_TSFILE_LIMIT));
  }

  @Test
  public void testPreprocessParametersInjectsEventUserForExternalWriteBackSink() {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(PipeSourceConstant.EXTRACTOR_KEY, "external-source");

    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(
        PipeSinkConstant.CONNECTOR_KEY, BuiltinPipePlugin.WRITE_BACK_CONNECTOR.getPipePluginName());
    final PipeParameters sinkParameters = new PipeParameters(sinkAttributes);

    PipeDataNodeTaskBuilder.preprocessParameters(
        new PipeParameters(sourceAttributes), sinkParameters);

    Assert.assertEquals(
        Boolean.TRUE.toString(),
        sinkParameters.getStringByKeys(PipeSinkConstant.CONNECTOR_USE_EVENT_USER_NAME_KEY));
  }
}
