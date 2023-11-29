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

package org.apache.iotdb.db.pipe.agent.plugin;

import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.plugin.builtin.processor.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.db.pipe.processor.downsampling.DownSamplingProcessor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

public class PipeProcessorConstructor extends PipePluginConstructor {
  PipeProcessorConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected void initConstructors() {
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName(), DoNothingProcessor::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.DOWN_SAMPLING_PROCESSOR.getPipePluginName(), DownSamplingProcessor::new);
  }

  @Override
  PipeProcessor reflectPlugin(PipeParameters processorParameters) {
    return (PipeProcessor)
        reflectPluginByKey(
            processorParameters
                .getStringOrDefault(
                    PipeProcessorConstant.PROCESSOR_KEY,
                    BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName())
                // Convert the value of `PROCESSOR_KEY` to lowercase for matching
                // `DO_NOTHING_PROCESSOR`
                .toLowerCase());
  }
}
