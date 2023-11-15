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

import org.apache.iotdb.commons.pipe.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public abstract class PipePluginConstructor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginConstructor.class);

  private final DataNodePipePluginMetaKeeper pipePluginMetaKeeper;

  protected static final Map<String, Supplier<PipePlugin>> PLUGIN_CONSTRUCTORS = new HashMap<>();

  PipePluginConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    this.pipePluginMetaKeeper = pipePluginMetaKeeper;
    initConstructors();
  }

  // New plugins shall be put here
  protected abstract void initConstructors();

  abstract PipePlugin reflectPlugin(PipeParameters pipeParameters);

  protected final PipePlugin reflectPluginByKey(String pluginKey) {
    return PLUGIN_CONSTRUCTORS.getOrDefault(pluginKey, () -> reflect(pluginKey)).get();
  }

  private PipePlugin reflect(String pluginName) {
    PipePluginMeta information = pipePluginMetaKeeper.getPipePluginMeta(pluginName);
    if (information == null) {
      String errorMessage =
          String.format(
              "Failed to reflect PipePlugin instance, because "
                  + "PipePlugin %s has not been registered.",
              pluginName.toUpperCase());
      LOGGER.warn(errorMessage);
      throw new PipeException(errorMessage);
    }

    try {
      return (PipePlugin)
          pipePluginMetaKeeper.getPluginClass(pluginName).getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException e) {
      String errorMessage =
          String.format(
              "Failed to reflect PipePlugin %s(%s) instance, because %s",
              pluginName, information.getClassName(), e);
      LOGGER.warn(errorMessage, e);
      throw new PipeException(errorMessage);
    }
  }
}
