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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.external.api.IPropertiesLoader;
import org.apache.iotdb.external.api.ISeriesNumerLimiter;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

public class JarLoaderUtil {

  private static final Logger logger = LoggerFactory.getLogger(JarLoaderUtil.class);

  public static URL[] getExternalJarURLs(String jarDir) throws IOException {
    HashSet<File> fileSet =
        new HashSet<>(
            FileUtils.listFiles(
                SystemFileFactory.INSTANCE.getFile(jarDir), new String[] {"jar"}, true));
    return FileUtils.toURLs(fileSet.toArray(new File[0]));
  }

  public static void loadExternLib(IoTDBConfig config) {
    // load external properties
    String loaderDir = config.getExternalPropertiesLoaderDir();

    if (!(new File(loaderDir).exists())) {
      return;
    }

    Path externalPropertiesFile = IoTDBDescriptor.getInstance().getExternalPropsPath();
    URL[] loaderJarURLs;
    List<Properties> externalPropertiesList = new ArrayList<>();
    try {
      loaderJarURLs = getExternalJarURLs(loaderDir);

      if (loaderJarURLs == null || loaderJarURLs.length == 0) {
        return;
      }

      ClassLoader classLoader = new URLClassLoader(loaderJarURLs);

      // Use SPI to get all plugins' class
      ServiceLoader<IPropertiesLoader> loaders =
          ServiceLoader.load(IPropertiesLoader.class, classLoader);

      for (IPropertiesLoader loader : loaders) {
        if (loader == null) {
          logger.error("IPropertiesLoader(), loader is null.");
          continue;
        }
        Properties properties = loader.loadProperties(externalPropertiesFile.toAbsolutePath());
        if (properties != null) {
          externalPropertiesList.add(properties);
        }
      }
    } catch (Throwable t) {
      logger.error("error happened while loading external loader. ", t);
      // ignore
    }

    if (externalPropertiesList.size() != 1) {
      return;
    }

    // overwrite the default properties;
    for (Properties properties : externalPropertiesList) {
      IoTDBDescriptor.getInstance().loadProperties(properties);
      TSFileDescriptor.getInstance()
          .overwriteConfigByCustomSettings(TSFileDescriptor.getInstance().getConfig(), properties);
    }

    String limiterDir = config.getExternalLimiterDir();

    if (!(new File(loaderDir).exists())) {
      return;
    }

    URL[] limiterJarURLs;

    List<ISeriesNumerLimiter> limiterList = new ArrayList<>();

    try {
      limiterJarURLs = getExternalJarURLs(limiterDir);

      if (limiterJarURLs == null || limiterJarURLs.length == 0) {
        return;
      }

      ClassLoader classLoader = new URLClassLoader(limiterJarURLs);

      // Use SPI to get all plugins' class
      ServiceLoader<ISeriesNumerLimiter> limiters =
          ServiceLoader.load(ISeriesNumerLimiter.class, classLoader);

      for (ISeriesNumerLimiter limiter : limiters) {
        if (limiter == null) {
          logger.error("ISeriesNumerLimiter(), limiter is null.");
          continue;
        }
        for (Properties properties : externalPropertiesList) {
          limiter.init(properties);
        }
        limiterList.add(limiter);
      }
    } catch (Throwable t) {
      // ignore
      logger.error("error happened while loading external limiter. ", t);
    }

    if (limiterList.size() != 1) {
      return;
    }

    SchemaEngine.getInstance().setSeriesNumerLimiter(limiterList.get(0));
  }
}
