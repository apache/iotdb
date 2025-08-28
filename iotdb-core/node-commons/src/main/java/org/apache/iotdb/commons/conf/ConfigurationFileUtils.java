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

package org.apache.iotdb.commons.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

public class ConfigurationFileUtils {

  private static final String lockFileSuffix = ".lock";
  private static final long maxTimeMillsToAcquireLock = TimeUnit.SECONDS.toMillis(20);
  private static final long waitTimeMillsPerCheck = TimeUnit.MILLISECONDS.toMillis(100);
  private static Logger logger = LoggerFactory.getLogger(ConfigurationFileUtils.class);
  private static final String lineSeparator = "\n";
  private static final String license =
      new StringJoiner(lineSeparator)
          .add("# Licensed to the Apache Software Foundation (ASF) under one")
          .add("# or more contributor license agreements.  See the NOTICE file")
          .add("# distributed with this work for additional information")
          .add("# regarding copyright ownership.  The ASF licenses this file")
          .add("# to you under the Apache License, Version 2.0 (the")
          .add("# \"License\"); you may not use this file except in compliance")
          .add("# with the License.  You may obtain a copy of the License at")
          .add("#")
          .add("#     http://www.apache.org/licenses/LICENSE-2.0")
          .add("#")
          .add("# Unless required by applicable law or agreed to in writing,")
          .add("# software distributed under the License is distributed on an")
          .add("# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY")
          .add("# KIND, either express or implied.  See the License for the")
          .add("# specific language governing permissions and limitations")
          .add("# under the License.")
          .toString();
  private static final String EFFECTIVE_MODE = "effectiveMode:";
  private static final String DATATYPE = "Datatype:";
  private static final String EFFECTIVE_MODE_HOT_RELOAD = "hot_reload";
  private static final String EFFECTIVE_MODE_RESTART = "restart";
  private static final String EFFECTIVE_MODE_FIRST_START = "first_start";
  private static Map<String, DefaultConfigurationItem> configuration2DefaultValue;

  // This is a temporary implementations
  private static final Set<String> ignoreConfigKeys =
      new HashSet<>(
          Arrays.asList(
              "cn_internal_address",
              "cn_internal_port",
              "cn_consensus_port",
              "cn_seed_config_node",
              "dn_internal_address",
              "dn_internal_port",
              "dn_mpp_data_exchange_port",
              "dn_schema_region_consensus_port",
              "dn_data_region_consensus_port",
              "dn_seed_config_node",
              "dn_session_timeout_threshold",
              "config_node_consensus_protocol_class",
              "schema_replication_factor",
              "data_replication_factor",
              "data_region_consensus_protocol_class",
              "series_slot_num",
              "series_partition_executor_class",
              "time_partition_interval",
              "schema_engine_mode",
              "tag_attribute_flush_interval",
              "tag_attribute_total_size",
              "timestamp_precision",
              "iotdb_server_encrypt_decrypt_provider",
              "iotdb_server_encrypt_decrypt_provider_parameter",
              "pipe_lib_dir"));

  private static final Map<String, String> lastAppliedProperties = new HashMap<>();

  public static void updateLastAppliedProperties(TrimProperties properties) {
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String key = entry.getKey().toString();
      String value = entry.getValue() == null ? null : entry.getValue().toString();
      lastAppliedProperties.put(key, value);
    }
  }

  public static Map<String, String> getLastAppliedProperties() {
    return lastAppliedProperties;
  }

  public static void checkAndMayUpdate(
      URL systemUrl, URL configNodeUrl, URL dataNodeUrl, URL commonUrl)
      throws IOException, InterruptedException {
    if (systemUrl == null || configNodeUrl == null || dataNodeUrl == null || commonUrl == null) {
      return;
    }
    File systemFile = new File(systemUrl.getFile());
    File configNodeFile = new File(configNodeUrl.getFile());
    File dataNodeFile = new File(dataNodeUrl.getFile());
    File commonFile = new File(commonUrl.getFile());

    if (systemFile.exists()) {
      return;
    }
    boolean canUpdate = (configNodeFile.exists() || dataNodeFile.exists()) && commonFile.exists();
    if (!canUpdate) {
      return;
    }

    File lockFile = new File(systemFile.getPath() + lockFileSuffix);
    acquireTargetFileLock(lockFile);
    try {
      // other progress updated this file
      if (systemFile.exists()) {
        return;
      }
      try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw")) {
        raf.write(license.getBytes());
        String configNodeContent = readConfigLinesWithoutLicense(configNodeFile);
        raf.write(configNodeContent.getBytes());
        String dataNodeContent = readConfigLinesWithoutLicense(dataNodeFile);
        raf.write(dataNodeContent.getBytes());
        String commonContent = readConfigLinesWithoutLicense(commonFile);
        raf.write(commonContent.getBytes());
      }
      Files.move(lockFile.toPath(), systemFile.toPath());
    } finally {
      releaseFileLock(lockFile);
    }
  }

  public static String readConfigFileContent(URL url) throws IOException {
    if (url == null) {
      return "";
    }
    File f = new File(url.getFile());
    if (!f.exists()) {
      return "";
    }
    return readConfigLines(f);
  }

  public static void loadConfigurationDefaultValueFromTemplate() throws IOException {
    if (configuration2DefaultValue != null) {
      return;
    }
    try {
      configuration2DefaultValue = getConfigurationItemsFromTemplate(false);
    } catch (IOException e) {
      logger.warn("Failed to read configuration template", e);
      throw e;
    }
  }

  // This function is used to get the default value for the configuration that enables hot
  // modification.
  public static String getConfigurationDefaultValue(String parameterName) throws IOException {
    parameterName = parameterName.trim();
    if (configuration2DefaultValue != null) {
      return configuration2DefaultValue.get(parameterName).value;
    } else {
      loadConfigurationDefaultValueFromTemplate();
      DefaultConfigurationItem defaultConfigurationItem =
          configuration2DefaultValue.get(parameterName);
      return defaultConfigurationItem == null ? null : defaultConfigurationItem.value;
    }
  }

  public static void releaseDefault() {
    configuration2DefaultValue = null;
  }

  public static String readConfigurationTemplateFile() throws IOException {
    StringBuilder content = new StringBuilder();
    try (InputStream inputStream =
            ConfigurationFileUtils.class
                .getClassLoader()
                .getResourceAsStream(CommonConfig.SYSTEM_CONFIG_TEMPLATE_NAME);
        InputStreamReader isr = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(isr)) {
      String line;
      while ((line = reader.readLine()) != null) {
        content.append(line).append(lineSeparator);
      }
    } catch (IOException e) {
      logger.warn("Failed to read configuration template", e);
      throw e;
    }
    return content.toString();
  }

  public static List<String> filterInvalidConfigItems(Map<String, String> configItems) {
    boolean successLoadDefaultValueMap = true;
    try {
      loadConfigurationDefaultValueFromTemplate();
    } catch (IOException e) {
      successLoadDefaultValueMap = false;
    }

    List<String> ignoredConfigItems = new ArrayList<>();
    for (String key : configItems.keySet()) {
      if (ignoreConfigKeys.contains(key)) {
        ignoredConfigItems.add(key);
      }
      if (successLoadDefaultValueMap && !configuration2DefaultValue.containsKey(key)) {
        ignoredConfigItems.add(key);
      }
    }
    ignoredConfigItems.forEach(configItems::remove);
    return ignoredConfigItems;
  }

  public static void updateConfiguration(
      File file, Properties newConfigItems, LoadHotModifiedPropsFunc loadHotModifiedPropertiesFunc)
      throws IOException, InterruptedException {
    File lockFile = new File(file.getPath() + lockFileSuffix);
    acquireTargetFileLock(lockFile);
    try {
      // read configuration file
      List<String> lines = new ArrayList<>();
      TrimProperties mergedProps = new TrimProperties();
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          lines.add(line);
          mergedProps.load(new StringReader(line));
        }
      }
      // overwrite old configuration with new value
      for (String key : newConfigItems.stringPropertyNames()) {
        mergedProps.put(key, newConfigItems.getProperty(key));
      }

      // load hot modified properties
      if (loadHotModifiedPropertiesFunc != null) {
        loadHotModifiedPropertiesFunc.loadHotModifiedProperties(mergedProps);
      }

      // generate new configuration file content in memory
      StringBuilder contentsOfNewConfigurationFile = new StringBuilder();
      for (String currentLine : lines) {
        if (currentLine.trim().isEmpty() || currentLine.trim().startsWith("#")) {
          contentsOfNewConfigurationFile.append(currentLine).append(lineSeparator);
          continue;
        }
        int equalsIndex = currentLine.indexOf('=');
        // replace old config
        if (equalsIndex != -1) {
          String key = currentLine.substring(0, equalsIndex).trim();
          String value = currentLine.substring(equalsIndex + 1).trim();
          if (!newConfigItems.containsKey(key)) {
            contentsOfNewConfigurationFile.append(currentLine).append(lineSeparator);
            continue;
          }
          if (newConfigItems.getProperty(key).equals(value)) {
            contentsOfNewConfigurationFile.append(currentLine).append(lineSeparator);
            newConfigItems.remove(key);
          } else {
            contentsOfNewConfigurationFile.append("#").append(currentLine).append(lineSeparator);
          }
        }
      }
      if (newConfigItems.isEmpty()) {
        // No configuration needs to be modified
        return;
      }
      logger.info("Updating configuration file {}", file.getAbsolutePath());
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(lockFile))) {
        writer.write(contentsOfNewConfigurationFile.toString());
        // Properties.store is not used as Properties.store may generate '\' automatically
        writer.write("#" + new Date().toString() + lineSeparator);
        for (String key : newConfigItems.stringPropertyNames()) {
          writer.write(key + "=" + newConfigItems.get(key) + lineSeparator);
        }
        writer.flush();
      }
      Files.move(lockFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    } finally {
      releaseFileLock(lockFile);
    }
  }

  private static String readConfigLinesWithoutLicense(File file) throws IOException {
    if (!file.exists()) {
      return "";
    }
    byte[] bytes = Files.readAllBytes(file.toPath());
    String content = new String(bytes);
    return lineSeparator + content.replace(license, "");
  }

  private static String readConfigLines(File file) throws IOException {
    if (!file.exists()) {
      return "";
    }
    byte[] bytes = Files.readAllBytes(file.toPath());
    return new String(bytes);
  }

  private static void acquireTargetFileLock(File file) throws IOException, InterruptedException {
    long totalWaitTime = 0;
    while (totalWaitTime < maxTimeMillsToAcquireLock) {
      if (file.createNewFile()) {
        return;
      }
      totalWaitTime += waitTimeMillsPerCheck;
      Thread.sleep(waitTimeMillsPerCheck);
    }
    logger.warn(
        "Waiting for {} seconds to acquire configuration file update lock."
            + " There may have been an unexpected interruption in the last"
            + " configuration file update. Ignore temporary file {}",
        totalWaitTime / 1000,
        file.getName());
  }

  private static void releaseFileLock(File file) throws IOException {
    Files.deleteIfExists(file.toPath());
  }

  @FunctionalInterface
  public interface LoadHotModifiedPropsFunc {
    void loadHotModifiedProperties(TrimProperties properties)
        throws IOException, InterruptedException;
  }

  public static Map<String, DefaultConfigurationItem> getConfigurationItemsFromTemplate(
      boolean withDesc) throws IOException {
    if (configuration2DefaultValue != null && !withDesc) {
      return configuration2DefaultValue;
    }
    Map<String, DefaultConfigurationItem> items = new LinkedHashMap<>();
    try (InputStream inputStream =
            ConfigurationFileUtils.class
                .getClassLoader()
                .getResourceAsStream(CommonConfig.SYSTEM_CONFIG_TEMPLATE_NAME);
        InputStreamReader isr = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(isr)) {
      String effectiveMode = null;
      String dataType = null;
      StringBuilder description = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty()) {
          description = new StringBuilder();
          dataType = null;
          effectiveMode = null;
          continue;
        }
        if (line.startsWith("#")) {
          String comment = line.substring(1).trim();
          if (description.length() > 0) {
            if (comment.startsWith(EFFECTIVE_MODE)) {
              effectiveMode = comment.substring(EFFECTIVE_MODE.length()).trim();
              continue;
            } else if (comment.startsWith(DATATYPE)) {
              dataType = comment.substring(DATATYPE.length()).trim();
              continue;
            } else {
              description.append(" ");
            }
          }
          if (withDesc) {
            description.append(comment);
          }
        } else {
          int equalsIndex = line.indexOf('=');
          String key = line.substring(0, equalsIndex).trim();
          String value = line.substring(equalsIndex + 1).trim();
          items.put(
              key,
              new DefaultConfigurationItem(
                  key,
                  value,
                  withDesc ? description.toString().trim() : null,
                  effectiveMode,
                  dataType));
        }
      }
    } catch (IOException e) {
      logger.warn("Failed to read configuration template", e);
      throw e;
    }
    return items;
  }

  public static class DefaultConfigurationItem {
    public String name;
    public String value;
    public String description;
    public String effectiveMode;
    public String dataType;

    public DefaultConfigurationItem(
        String name, String value, String description, String effectiveMode, String dataType) {
      this.name = name;
      this.value = value;
      this.description = description;
      this.effectiveMode = effectiveMode;
      this.dataType = dataType;
    }
  }
}
