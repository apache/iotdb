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
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.source.donothing.DoNothingSource;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeSourceConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource;

class PipeDataRegionSourceConstructor extends PipeSourceConstructor {

  PipeDataRegionSourceConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected void initConstructors() {
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_EXTRACTOR.getPipePluginName(), DoNothingSource::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName(), IoTDBDataRegionSource::new);

    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_SOURCE.getPipePluginName(), DoNothingSource::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_SOURCE.getPipePluginName(), IoTDBDataRegionSource::new);
  }
}
