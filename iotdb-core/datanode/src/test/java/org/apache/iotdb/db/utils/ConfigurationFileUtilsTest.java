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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public class ConfigurationFileUtilsTest {

  private File dir =
      new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "ConfigurationFileUtilsTest");

  @After
  public void tearDown() throws IOException {
    EnvironmentUtils.cleanDir(dir.getPath());
  }

  @Test
  public void testMergeOldVersionFiles() throws IOException, InterruptedException {
    dir.mkdirs();
    File confignodeConfigFile = new File(dir + File.separator + "iotdb-confignode.properties");
    File datanodeConfigFile = new File(dir + File.separator + "iotdb-datanode.properties");
    File commonConfigFile = new File(dir + File.separator + "iotdb-common.properties");
    File systemConfigFile = new File(dir + File.separator + "iotdb-system.properties");
    generateFile(confignodeConfigFile, "a=1");
    generateFile(datanodeConfigFile, "b=2");
    generateFile(commonConfigFile, "c=3");
    ConfigurationFileUtils.checkAndMayUpdate(
        systemConfigFile.toURI().toURL(),
        confignodeConfigFile.toURI().toURL(),
        datanodeConfigFile.toURI().toURL(),
        commonConfigFile.toURI().toURL());
    Assert.assertTrue(systemConfigFile.exists());
    Properties properties = new Properties();
    try (FileReader fileReader = new FileReader(systemConfigFile)) {
      properties.load(fileReader);
    }
    Assert.assertEquals("1", properties.getProperty("a"));
    Assert.assertEquals("2", properties.getProperty("b"));
    Assert.assertEquals("3", properties.getProperty("c"));
  }

  @Test
  public void checkIoTDBSystemTemplateFileFormat() throws IOException {
    Map<String, ConfigurationFileUtils.DefaultConfigurationItem> configurationItemsFromTemplate =
        ConfigurationFileUtils.getConfigurationItemsFromTemplate(false);
    for (Map.Entry<String, ConfigurationFileUtils.DefaultConfigurationItem> entry :
        configurationItemsFromTemplate.entrySet()) {
      String key = entry.getKey();
      ConfigurationFileUtils.DefaultConfigurationItem value = entry.getValue();
      Assert.assertFalse(
          "The format of configuration item [" + key + "] is incorrect",
          value.effectiveMode == ConfigurationFileUtils.EffectiveModeType.UNKNOWN);
    }
  }

  /**
   * Regression guard for V2-995: a configuration item whose {@code effectiveMode} allows {@code set
   * configuration} (i.e. anything except {@code FIRST_START}) must have its {@code key=value} line
   * left uncommented in the template. {@code getConfigurationItemsFromTemplate} only parses
   * uncommented lines, so a commented item never enters {@code configuration2DefaultValue}; {@code
   * filterInvalidConfigItems} then treats it as undefined and {@code set configuration} rejects it
   * with "immutable or undefined" — silently disabling the advertised dynamic-config entry point.
   * This was the root cause for {@code enable_topology_probing} (hot_reload) and the two {@code
   * topology_probing_*} items (restart).
   */
  @Test
  public void checkSettableItemsAreUncommentedInTemplate() throws IOException {
    // Keys whose value line appears uncommented at least once. Some keys (e.g. dn_data_dirs) list a
    // commented Windows-path variant next to an uncommented Unix-path variant; only the uncommented
    // one is parsed, so such keys are fine and must not be flagged.
    Set<String> uncommentedKeys = new HashSet<>();
    // Keys with a commented value line whose enclosing block declares a settable effectiveMode.
    Set<String> commentedSettableKeys = new HashSet<>();
    try (InputStream inputStream =
            ConfigurationFileUtilsTest.class
                .getClassLoader()
                .getResourceAsStream(CommonConfig.SYSTEM_CONFIG_TEMPLATE_NAME);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(Objects.requireNonNull(inputStream)))) {
      String line;
      ConfigurationFileUtils.EffectiveModeType currentMode = null;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty()) {
          // Blank line separates configuration blocks; reset the accumulated effectiveMode.
          currentMode = null;
          continue;
        }
        if (line.startsWith("# effectiveMode:")) {
          currentMode =
              ConfigurationFileUtils.EffectiveModeType.getEffectiveMode(
                  line.substring("# effectiveMode:".length()).trim());
          continue;
        }
        // Detect the (possibly commented) "key=value" line that closes a block.
        String stripped = line.startsWith("#") ? line.substring(1).trim() : line;
        int equalsIndex = stripped.indexOf('=');
        boolean isValueLine =
            equalsIndex > 0 && stripped.substring(0, equalsIndex).trim().matches("[a-zA-Z0-9_.]+");
        if (!isValueLine) {
          continue;
        }
        String key = stripped.substring(0, equalsIndex).trim();
        if (line.startsWith("#")) {
          // FIRST_START items are intentionally rejected by 'set configuration'; UNKNOWN items have
          // no declared mode. Only flag items that 'set configuration' is supposed to accept.
          if (isSettableByConfiguration(currentMode)) {
            commentedSettableKeys.add(key);
          }
        } else {
          uncommentedKeys.add(key);
        }
        // A value line ends the current block's effectiveMode scope.
        currentMode = null;
      }
    }
    commentedSettableKeys.removeAll(uncommentedKeys);
    Assert.assertTrue(
        "configuration items settable via 'set configuration' must be uncommented in the template "
            + "so the command can reach them instead of rejecting them as undefined; "
            + "commented settable items found: "
            + commentedSettableKeys,
        commentedSettableKeys.isEmpty());
  }

  private static boolean isSettableByConfiguration(ConfigurationFileUtils.EffectiveModeType mode) {
    return mode == ConfigurationFileUtils.EffectiveModeType.HOT_RELOAD
        || mode == ConfigurationFileUtils.EffectiveModeType.RESTART
        || mode == ConfigurationFileUtils.EffectiveModeType.FIRST_START_OR_SET_CONFIGURATION;
  }

  private void generateFile(File file, String content) throws IOException {
    Files.write(file.toPath(), content.getBytes());
  }
}
