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

package org.apache.iotdb.db.sync.externalpipe;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** ExtPipePluginRegister is used to manage the all External Pipe Plugin info. */
public class ExtPipePluginRegister {
  private static final Logger logger = LoggerFactory.getLogger(ExtPipePluginRegister.class);

  // the dir saving external pipe plugin .jar files
  private final String extPipeDir;
  // Map: pluginName => IExternalPipeSinkWriterFactory
  private final Map<String, IExternalPipeSinkWriterFactory> writerFactoryMap =
      new ConcurrentHashMap<>();

  private ExtPipePluginRegister() throws IOException {
    extPipeDir = IoTDBDescriptor.getInstance().getConfig().getExtPipeDir();
    logger.info("extPipeDir: {}", extPipeDir);

    makeExtPipeDir();
    buildFactoryMap();
  }

  public boolean pluginExist(String pluginName) {
    return writerFactoryMap.containsKey(pluginName.toLowerCase());
  }

  public IExternalPipeSinkWriterFactory getWriteFactory(String pluginName) {
    return writerFactoryMap.get(pluginName.toLowerCase());
  }

  public Set<String> getAllPluginName() {
    return writerFactoryMap.keySet();
  }

  private void makeExtPipeDir() throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(extPipeDir);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private Collection<File> findAllJar(File directory) {
    try (Stream<File> stream = FileUtils.streamFiles(directory, true, "jar")) {
      return stream.collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException(directory.toString(), e);
    }
  }

  private URL[] getPlugInJarURLs() throws IOException {
    HashSet<File> fileSet =
        new HashSet<>(findAllJar(SystemFileFactory.INSTANCE.getFile(extPipeDir)));
    return FileUtils.toURLs(fileSet.toArray(new File[0]));
  }

  private void buildFactoryMap() throws IOException {
    URL[] jarURLs = getPlugInJarURLs();
    logger.debug("ExtPipePluginRegister buildFactoryMap(), ExtPIPE Plugin jarURLs: {}", jarURLs);

    for (URL jarUrl : jarURLs) {
      ClassLoader classLoader = new URLClassLoader(new URL[] {jarUrl});

      // Use SPI to get all plugins' class
      ServiceLoader<IExternalPipeSinkWriterFactory> factories =
          ServiceLoader.load(IExternalPipeSinkWriterFactory.class, classLoader);

      for (IExternalPipeSinkWriterFactory factory : factories) {
        if (factory == null) {
          logger.error("ExtPipePluginRegister buildFactoryMap(), factory is null.");
          continue;
        }

        String pluginName = factory.getExternalPipeType().toLowerCase();
        writerFactoryMap.put(pluginName, factory);
        logger.info(
            "ExtPipePluginRegister buildFactoryMap(), find ExternalPipe Plugin {}.", pluginName);
      }
    }
  }

  // == singleton mode
  public static ExtPipePluginRegister getInstance() {
    return ExtPipePluginRegisterHolder.INSTANCE;
  }

  private static class ExtPipePluginRegisterHolder {
    private static ExtPipePluginRegister INSTANCE = null;

    static {
      try {
        INSTANCE = new ExtPipePluginRegister();
      } catch (IOException e) {
        logger.error("new ExtPipePluginRegister() error.", e);
      }
    }

    private ExtPipePluginRegisterHolder() {}
  }
}
