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

import org.apache.iotdb.commons.pipe.agent.plugin.PipePluginConstructor;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.db.pipe.extractor.IoTDBSchemaRegionExtractor;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Arrays;

public class PipeSchemaRegionExtractorConstructor extends PipePluginConstructor {

  PipeSchemaRegionExtractorConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected void initConstructors() {
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_SCHEMA_REGION_EXTRACTOR.getPipePluginName(),
        IoTDBSchemaRegionExtractor::new);

    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_SCHEMA_REGION_SOURCE.getPipePluginName(),
        IoTDBSchemaRegionExtractor::new);
  }

  @Override
  protected PipeExtractor reflectPlugin(PipeParameters extractorParameters) {
    if (!extractorParameters.hasAnyAttributes(
        PipeExtractorConstant.EXTRACTOR_KEY, PipeExtractorConstant.SOURCE_KEY)) {
      throw new PipeException(
          "Failed to reflect PipeExtractor instance because "
              + "'extractor' is not specified in the parameters.");
    }

    return (PipeExtractor)
        reflectPluginByKey(
            extractorParameters
                .getStringOrDefault(
                    Arrays.asList(
                        PipeExtractorConstant.EXTRACTOR_KEY, PipeExtractorConstant.SOURCE_KEY),
                    BuiltinPipePlugin.IOTDB_SCHEMA_REGION_SOURCE.getPipePluginName())
                // Convert the value of `EXTRACTOR_KEY` or `SOURCE_KEY` to lowercase for matching in
                // `PLUGIN_CONSTRUCTORS`
                .toLowerCase());
  }

  @Override
  protected final PipePlugin reflectPluginByKey(String pluginKey) {
    // currently only support IOTDB_SCHEMA_REGION_SOURCE
    return PLUGIN_CONSTRUCTORS
        .get(BuiltinPipePlugin.IOTDB_SCHEMA_REGION_SOURCE.getPipePluginName())
        .get();
  }
}
