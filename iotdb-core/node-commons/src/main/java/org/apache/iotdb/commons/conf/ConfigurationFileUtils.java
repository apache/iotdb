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

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;

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
import java.util.Collections;
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
  private static final Logger logger = LoggerFactory.getLogger(ConfigurationFileUtils.class);
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
  private static final String EFFECTIVE_MODE_PREFIX = "effectiveMode:";
  private static final String DATATYPE_PREFIX = "Datatype:";
  private static final String PRIVILEGE_PREFIX = "Privilege:";
  private static Map<String, DefaultConfigurationItem> configuration2DefaultValue;

  // Used to display in showConfigurationStatement
  private static final Map<String, String> lastAppliedProperties = new HashMap<>();
  private static final String displayValueOfHidedParameter = "******";
  private static final Set<String> hidedParameters = new HashSet<>();

  static {
    hidedParameters.add("key_store_pwd");
    hidedParameters.add("trust_store_pwd");
  }

  public static void updateAppliedProperties(Properties properties, boolean isHotReloading) {
    try {
      loadConfigurationDefaultValueFromTemplate();
    } catch (IOException e) {
      logger.error("Failed to update applied properties", e);
      return;
    }
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String key = entry.getKey().toString();
      DefaultConfigurationItem defaultConfigurationItem = configuration2DefaultValue.get(key);
      if (defaultConfigurationItem == null) {
        continue;
      }
      if (isHotReloading
          && defaultConfigurationItem.effectiveMode != EffectiveModeType.HOT_RELOAD) {
        continue;
      }
      String value = entry.getValue() == null ? null : entry.getValue().toString();
      lastAppliedProperties.put(
          key, hidedParameters.contains(key) ? displayValueOfHidedParameter : value);
    }
  }

  public static void updateAppliedPropertiesFromCN(TSystemConfigurationResp resp) {
    if (resp.getGlobalConfig().isSetTimestampPrecision()) {
      lastAppliedProperties.put(
          "timestamp_precision", resp.getGlobalConfig().getTimestampPrecision());
    }
    if (resp.getGlobalConfig().isSetTimePartitionInterval()) {
      lastAppliedProperties.put(
          "time_partition_interval",
          String.valueOf(resp.getGlobalConfig().getTimePartitionInterval()));
    }
    if (resp.getGlobalConfig().isSetTimePartitionOrigin()) {
      lastAppliedProperties.put(
          "time_partition_origin", String.valueOf(resp.getGlobalConfig().getTimePartitionOrigin()));
    }
    if (resp.getGlobalConfig().isSetSchemaEngineMode()) {
      lastAppliedProperties.put("schema_engine_mode", resp.getGlobalConfig().getSchemaEngineMode());
    }
    if (resp.getGlobalConfig().isSetTagAttributeTotalSize()) {
      lastAppliedProperties.put(
          "tag_attribute_total_size",
          String.valueOf(resp.getGlobalConfig().getTagAttributeTotalSize()));
    }
    if (resp.getGlobalConfig().isSetSeriesPartitionExecutorClass()) {
      lastAppliedProperties.put(
          "series_partition_executor_class",
          resp.getGlobalConfig().getSeriesPartitionExecutorClass());
    }
    if (resp.getGlobalConfig().isSetSeriesPartitionSlotNum()) {
      lastAppliedProperties.put(
          "series_slot_num", String.valueOf(resp.getGlobalConfig().getSeriesPartitionSlotNum()));
    }
    if (resp.getGlobalConfig().isSetDataRegionConsensusProtocolClass()) {
      lastAppliedProperties.put(
          "data_region_consensus_protocol_class",
          resp.getGlobalConfig().getDataRegionConsensusProtocolClass());
    }
    if (resp.getGlobalConfig().isSetSchemaRegionConsensusProtocolClass()) {
      lastAppliedProperties.put(
          "schema_region_consensus_protocol_class",
          resp.getGlobalConfig().getSchemaRegionConsensusProtocolClass());
    }
    if (resp.getGlobalConfig().isSetReadConsistencyLevel()) {
      lastAppliedProperties.put(
          "read_consistency_level", resp.getGlobalConfig().getReadConsistencyLevel());
    }
    if (resp.getGlobalConfig().isSetDiskSpaceWarningThreshold()) {
      lastAppliedProperties.put(
          "disk_space_warning_threshold",
          String.valueOf(resp.getGlobalConfig().getDiskSpaceWarningThreshold()));
    }
  }

  // This method may not be used in the current version directly, but should not be removed to
  // reduce conflicts
  @SuppressWarnings("unused")
  public static void updateAppliedProperties(String key, String value) {
    lastAppliedProperties.put(key, value);
  }

  public static Map<String, String> getAppliedProperties() {
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
    if (configuration2DefaultValue == null) {
      loadConfigurationDefaultValueFromTemplate();
    }
    DefaultConfigurationItem defaultConfigurationItem =
        configuration2DefaultValue.get(parameterName);
    return defaultConfigurationItem == null ? null : defaultConfigurationItem.value;
  }

  public static PrivilegeType getConfigurationItemPrivilege(String parameterName)
      throws IOException {
    parameterName = parameterName.trim();
    if (configuration2DefaultValue == null) {
      loadConfigurationDefaultValueFromTemplate();
    }
    DefaultConfigurationItem defaultConfigurationItem =
        configuration2DefaultValue.get(parameterName);
    return defaultConfigurationItem == null ? null : defaultConfigurationItem.privilege;
  }

  public static boolean parameterNeedKeepConsistentInCluster(String key) {
    return false;
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
    if (!successLoadDefaultValueMap) {
      return Collections.emptyList();
    }

    List<String> ignoredConfigItems = new ArrayList<>();
    for (String key : configItems.keySet()) {
      DefaultConfigurationItem defaultConfigurationItem = configuration2DefaultValue.get(key);
      if (defaultConfigurationItem == null
          || defaultConfigurationItem.effectiveMode == EffectiveModeType.FIRST_START) {
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
      List<String> independentLines = new ArrayList<>();
      EffectiveModeType effectiveMode = null;
      PrivilegeType privilege = null;
      StringBuilder description = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        // Clean up when encountering a blank line.
        // For some parameters that are continuous, they share the same properties
        // example:
        // # min election timeout for leader election
        // # effectiveMode: restart
        // # Datatype: int
        // config_node_ratis_rpc_leader_election_timeout_min_ms=2000
        // schema_region_ratis_rpc_leader_election_timeout_min_ms=2000
        if (line.isEmpty()) {
          description = new StringBuilder();
          effectiveMode = null;
          privilege = null;
          independentLines.clear();
          continue;
        }
        if (line.startsWith("#")) {
          String comment = line.substring(1).trim();
          if (comment.isEmpty()) {
            continue;
          }
          if (comment.startsWith(EFFECTIVE_MODE_PREFIX)) {
            effectiveMode =
                EffectiveModeType.getEffectiveMode(
                    comment.substring(EFFECTIVE_MODE_PREFIX.length()).trim());
            independentLines.add(comment);
            continue;
          } else if (comment.startsWith(DATATYPE_PREFIX)) {
            independentLines.add(comment);
            continue;
          } else if (comment.startsWith(PRIVILEGE_PREFIX)) {
            privilege =
                PrivilegeType.valueOf(
                    comment.substring(PRIVILEGE_PREFIX.length()).trim().toUpperCase());
            independentLines.add(comment);
            continue;
          } else {
            description.append(" ");
          }
          if (withDesc) {
            description.append(comment);
          }
        } else {
          int equalsIndex = line.indexOf('=');
          if (equalsIndex == -1) {
            // Skip lines without '=' to avoid StringIndexOutOfBoundsException
            continue;
          }
          String key = line.substring(0, equalsIndex).trim();
          String value = line.substring(equalsIndex + 1).trim();
          for (String independentLine : independentLines) {
            description.append(lineSeparator).append(independentLine);
          }
          items.put(
              key,
              new DefaultConfigurationItem(
                  key,
                  value,
                  withDesc ? description.toString().trim() : null,
                  effectiveMode,
                  privilege));
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
    public EffectiveModeType effectiveMode;
    public PrivilegeType privilege;

    public DefaultConfigurationItem(
        String name,
        String value,
        String description,
        EffectiveModeType effectiveMode,
        PrivilegeType privilegeType) {
      this.name = name;
      this.value = value;
      this.description = description;
      this.effectiveMode = effectiveMode == null ? EffectiveModeType.UNKNOWN : effectiveMode;
      this.privilege = privilegeType == null ? PrivilegeType.SYSTEM : privilegeType;
    }
  }

  public enum EffectiveModeType {
    HOT_RELOAD,
    FIRST_START,
    FIRST_START_OR_SET_CONFIGURATION,
    RESTART,
    UNKNOWN;

    public static EffectiveModeType getEffectiveMode(String effectiveMode) {
      if (HOT_RELOAD.name().equalsIgnoreCase(effectiveMode)) {
        return HOT_RELOAD;
      } else if (FIRST_START.name().equalsIgnoreCase(effectiveMode)) {
        return FIRST_START;
      } else if (FIRST_START_OR_SET_CONFIGURATION.name().equalsIgnoreCase(effectiveMode)) {
        return FIRST_START_OR_SET_CONFIGURATION;
      } else if (RESTART.name().equalsIgnoreCase(effectiveMode)) {
        return RESTART;
      } else {
        return UNKNOWN;
      }
    }
  }
}
