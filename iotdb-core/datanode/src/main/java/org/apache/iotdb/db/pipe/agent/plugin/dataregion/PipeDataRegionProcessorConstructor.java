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

package org.apache.iotdb.db.pipe.agent.plugin.dataregion;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.throwing.ThrowingExceptionProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeProcessorConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.db.pipe.processor.aggregate.AggregateProcessor;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.processor.StandardStatisticsOperatorProcessor;
import org.apache.iotdb.db.pipe.processor.aggregate.window.processor.TumblingWindowingProcessor;
import org.apache.iotdb.db.pipe.processor.downsampling.changing.ChangingValueSamplingProcessor;
import org.apache.iotdb.db.pipe.processor.downsampling.sdt.SwingingDoorTrendingSamplingProcessor;
import org.apache.iotdb.db.pipe.processor.downsampling.tumbling.TumblingTimeSamplingProcessor;
import org.apache.iotdb.db.pipe.processor.pipeconsensus.PipeConsensusProcessor;
import org.apache.iotdb.db.pipe.processor.schemachange.RenameDatabaseProcessor;
import org.apache.iotdb.db.pipe.processor.twostage.plugin.TwoStageCountProcessor;

class PipeDataRegionProcessorConstructor extends PipeProcessorConstructor {

  PipeDataRegionProcessorConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected void initConstructors() {
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName(), DoNothingProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.TUMBLING_TIME_SAMPLING_PROCESSOR.getPipePluginName(),
        TumblingTimeSamplingProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.SDT_SAMPLING_PROCESSOR.getPipePluginName(),
        SwingingDoorTrendingSamplingProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.CHANGING_VALUE_SAMPLING_PROCESSOR.getPipePluginName(),
        ChangingValueSamplingProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.THROWING_EXCEPTION_PROCESSOR.getPipePluginName(),
        ThrowingExceptionProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.AGGREGATE_PROCESSOR.getPipePluginName(), AggregateProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.STANDARD_STATISTICS_PROCESSOR.getPipePluginName(),
        StandardStatisticsOperatorProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.TUMBLING_WINDOWING_PROCESSOR.getPipePluginName(),
        TumblingWindowingProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.COUNT_POINT_PROCESSOR.getPipePluginName(), TwoStageCountProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.PIPE_CONSENSUS_PROCESSOR.getPipePluginName(),
        PipeConsensusProcessor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.RENAME_DATABASE_PROCESSOR.getPipePluginName(),
        RenameDatabaseProcessor::new);
  }
}
