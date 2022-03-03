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

package org.apache.iotdb.db.pipe.external;

import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/** ExternalPipePluginManager is used to manage the all External Pipe Plugin info. */
public class ExternalPipePluginManager {
  private static final Logger logger = LoggerFactory.getLogger(ExternalPipePluginManager.class);

  // Map: pluginName => IExternalPipeSinkWriterFactory
  private final Map<String, IExternalPipeSinkWriterFactory> writerFactoryMap =
      new ConcurrentHashMap<>();

  private ExternalPipePluginManager() {
    IExternalPipeSinkWriterFactory externalPipeSinkWriterFactory = null;

    // Use SPI to get all plugins' class
    ServiceLoader<IExternalPipeSinkWriterFactory> factories =
        ServiceLoader.load(IExternalPipeSinkWriterFactory.class);
    for (IExternalPipeSinkWriterFactory factory : factories) {
      if (factory == null) {
        logger.error("ExternalPipePluginManager(), factory is null.");
        continue;
      }

      String pluginName = factory.getExternalPipeType().toLowerCase();
      writerFactoryMap.put(pluginName, factory);
      logger.info("ExternalPipePluginManager(), find ExternalPipe Plugin {}.", pluginName);
    }
  }

  public boolean pluginExist(String pluginName) {
    return writerFactoryMap.containsKey(pluginName.toLowerCase());
  }

  public IExternalPipeSinkWriterFactory getWriteFactory(String pluginName) {
    return writerFactoryMap.get(pluginName.toLowerCase());
  }

  // == singleton mode
  public static ExternalPipePluginManager getInstance() {
    return ExternalPipePluginManager.ExternalPipePluginManagerHolder.INSTANCE;
  }

  private static class ExternalPipePluginManagerHolder {
    private static final ExternalPipePluginManager INSTANCE = new ExternalPipePluginManager();

    private ExternalPipePluginManagerHolder() {}
  }
}
