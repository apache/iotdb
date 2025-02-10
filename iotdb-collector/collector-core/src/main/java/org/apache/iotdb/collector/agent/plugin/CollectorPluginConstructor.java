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

package org.apache.iotdb.collector.agent.plugin;

import org.apache.iotdb.collector.plugin.BuiltinCollectorPlugin;
import org.apache.iotdb.collector.plugin.builtin.processor.DoNothingProcessor;
import org.apache.iotdb.collector.plugin.builtin.sink.SessionSink;
import org.apache.iotdb.collector.plugin.builtin.source.HttpSource;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.PipeSink;
import org.apache.iotdb.pipe.api.PipeSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class CollectorPluginConstructor {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectorPluginConstructor.class);

  protected final Map<String, Supplier<PipePlugin>> pluginConstructors = new HashMap<>();

  private CollectorPluginConstructor() {
    initConstructors();
  }

  private void initConstructors() {
    pluginConstructors.put(
        BuiltinCollectorPlugin.HTTP_SOURCE.getCollectorPluginName(), HttpSource::new);
    pluginConstructors.put(
        BuiltinCollectorPlugin.DO_NOTHING_PROCESSOR.getCollectorPluginName(),
        DoNothingProcessor::new);
    pluginConstructors.put(
        BuiltinCollectorPlugin.IOTDB_SESSION_SINK.getCollectorPluginName(), SessionSink::new);
    LOGGER.info("builtin plugin has been initialized");
  }

  public PipeSource getSource(final String pluginName) {
    return (PipeSource) pluginConstructors.get(pluginName).get();
  }

  public PipeProcessor getProcessor(final String pluginName) {
    return (PipeProcessor) pluginConstructors.get(pluginName).get();
  }

  public PipeSink getSink(final String pluginName) {
    return (PipeSink) pluginConstructors.get(pluginName).get();
  }

  public static CollectorPluginConstructor instance() {
    return CollectorPluginConstructorHolder.INSTANCE;
  }

  private static class CollectorPluginConstructorHolder {
    private static final CollectorPluginConstructor INSTANCE = new CollectorPluginConstructor();
  }
}
