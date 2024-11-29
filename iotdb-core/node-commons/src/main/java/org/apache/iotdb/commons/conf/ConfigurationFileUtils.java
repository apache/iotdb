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
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  private static Map<String, String> configuration2DefaultValue;

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

  public static void getConfigurationDefaultValue() throws IOException {
    if (configuration2DefaultValue != null) {
      return;
    }
    configuration2DefaultValue = new HashMap<>();
    try (InputStream inputStream =
            ConfigurationFileUtils.class
                .getClassLoader()
                .getResourceAsStream(CommonConfig.SYSTEM_CONFIG_TEMPLATE_NAME);
        InputStreamReader isr = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(isr)) {
      String line;
      Pattern pattern = Pattern.compile("^[^#].*?=.*$");
      while ((line = reader.readLine()) != null) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
          String[] parts = line.split("=");
          if (parts.length == 2) {
            configuration2DefaultValue.put(parts[0].trim(), parts[1].trim());
          } else if (parts.length == 1) {
            configuration2DefaultValue.put(parts[0].trim(), null);
          } else {
            logger.error("Failed to parse configuration template: {}", line);
          }
        }
      }
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
      return configuration2DefaultValue.get(parameterName);
    } else {
      getConfigurationDefaultValue();
      return configuration2DefaultValue.getOrDefault(parameterName, null);
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

  public static List<String> filterImmutableConfigItems(Map<String, String> configItems) {
    List<String> ignoredConfigItems = new ArrayList<>();
    for (String ignoredKey : ignoreConfigKeys) {
      if (configItems.containsKey(ignoredKey)) {
        configItems.remove(ignoredKey);
        ignoredConfigItems.add(ignoredKey);
      }
    }
    return ignoredConfigItems;
  }

  public static void updateConfigurationFile(File file, Properties newConfigItems)
      throws IOException, InterruptedException {
    File lockFile = new File(file.getPath() + lockFileSuffix);
    acquireTargetFileLock(lockFile);
    try {
      // read configuration file
      List<String> lines = new ArrayList<>();
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          lines.add(line);
        }
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
}
