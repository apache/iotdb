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

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.db.pipe.extractor.IoTDBDataRegionExtractor;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.Arrays;

public class PipeExtractorConstructor extends PipePluginConstructor {

  PipeExtractorConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected void initConstructors() {
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName(), IoTDBDataRegionExtractor::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_SOURCE.getPipePluginName(), IoTDBDataRegionExtractor::new);
  }

  @Override
  PipeExtractor reflectPlugin(PipeParameters extractorParameters) {
    return (PipeExtractor)
        reflectPluginByKey(
            extractorParameters
                .getStringOrDefault(
                    Arrays.asList(
                        PipeExtractorConstant.EXTRACTOR_KEY, PipeExtractorConstant.SOURCE_KEY),
                    BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
                // Convert the value of `EXTRACTOR_KEY` or `SOURCE_KEY` to lowercase for matching
                // `IOTDB_EXTRACTOR`
                .toLowerCase());
  }
}
