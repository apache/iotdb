/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.protocol.mqtt;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;

/** PayloadFormatManager loads payload formatter from SPI services. */
public class PayloadFormatManager {
  private static final Logger logger = LoggerFactory.getLogger(PayloadFormatManager.class);

  // The dir saving MQTT payload plugin .jar files
  private static String mqttDir;
  // Map: formatterName => PayloadFormatter
  private static Map<String, PayloadFormatter> mqttPayloadPluginMap = new HashMap<>();

  static {
    init();
  }

  private static void init() {
    mqttDir = IoTDBDescriptor.getInstance().getConfig().getMqttDir();
    logger.info("mqttDir: {}", mqttDir);

    try {
      makeMqttPluginDir();
      buildMqttPluginMap();
    } catch (IOException e) {
      logger.error("MQTT PayloadFormatManager init() error.", e);
    }
  }

  public static PayloadFormatter getPayloadFormat(String name) {
    PayloadFormatter formatter = mqttPayloadPluginMap.get(name);
    Preconditions.checkArgument(formatter != null, "Unknown payload format named: " + name);
    return formatter;
  }

  private static void makeMqttPluginDir() throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(mqttDir);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private static void buildMqttPluginMap() throws IOException {
    ServiceLoader<PayloadFormatter> payloadFormatters = ServiceLoader.load(PayloadFormatter.class);
    for (PayloadFormatter formatter : payloadFormatters) {
      if (formatter == null) {
        logger.error("PayloadFormatManager(), formatter is null.");
        continue;
      }

      String pluginName = formatter.getName();
      mqttPayloadPluginMap.put(pluginName, formatter);
      logger.info("PayloadFormatManager(), find MQTT Payload Plugin {}.", pluginName);
    }

    URL[] jarURLs = getPluginJarURLs(mqttDir);
    logger.debug("MQTT Plugin jarURLs: {}", jarURLs);

    for (URL jarUrl : jarURLs) {
      ClassLoader classLoader = new URLClassLoader(new URL[] {jarUrl});

      // Use SPI to get all plugins' class
      ServiceLoader<PayloadFormatter> payloadFormatters2 =
          ServiceLoader.load(PayloadFormatter.class, classLoader);

      for (PayloadFormatter formatter : payloadFormatters2) {
        if (formatter == null) {
          logger.error("PayloadFormatManager(), formatter is null.");
          continue;
        }

        String pluginName = formatter.getName();
        if (mqttPayloadPluginMap.containsKey(pluginName)) {
          continue;
        }
        mqttPayloadPluginMap.put(pluginName, formatter);
        logger.info("PayloadFormatManager(), find MQTT Payload Plugin {}.", pluginName);
      }
    }
  }

  /**
   * get all jar files in the given folder
   *
   * @param folderPath
   * @return all jar files' URL
   * @throws IOException
   */
  private static URL[] getPluginJarURLs(String folderPath) throws IOException {
    HashSet<File> fileSet =
        new HashSet<>(
            org.apache.commons.io.FileUtils.listFiles(
                SystemFileFactory.INSTANCE.getFile(folderPath), new String[] {"jar"}, true));
    return org.apache.commons.io.FileUtils.toURLs(fileSet.toArray(new File[0]));
  }
}
