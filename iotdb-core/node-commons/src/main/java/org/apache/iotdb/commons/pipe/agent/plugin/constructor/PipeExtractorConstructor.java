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

package org.apache.iotdb.commons.pipe.agent.plugin.constructor;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.Arrays;

public abstract class PipeExtractorConstructor extends PipePluginConstructor {

  protected PipeExtractorConstructor(PipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  protected PipeExtractorConstructor() {
    super();
  }

  @Override
  public final PipeExtractor reflectPlugin(PipeParameters extractorParameters) {
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
