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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.TrimProperties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TimeseriesDeletionExecutionIntervalHotReloadTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testConfigurationCanBeAppliedWithoutRestart() throws Exception {
    final ConfigurationFileUtils.DefaultConfigurationItem item =
        ConfigurationFileUtils.getConfigurationItemsFromTemplate(false)
            .get("timeseries_deletion_execution_interval_ms");

    assertNotNull(item);
    assertEquals(ConfigurationFileUtils.EffectiveModeType.HOT_RELOAD, item.effectiveMode);
  }

  @Test
  public void testHotReloadUpdatesExecutionInterval() throws Exception {
    final String originalConf = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF);
    final File confDir = temporaryFolder.newFolder();
    Files.writeString(
        confDir.toPath().resolve(CommonConfig.SYSTEM_CONFIG_NAME),
        "cn_seed_config_node=127.0.0.1:10710\n",
        StandardCharsets.UTF_8);
    System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, confDir.getAbsolutePath());
    try {
      final Constructor<ConfigNodeDescriptor> constructor =
          ConfigNodeDescriptor.class.getDeclaredConstructor();
      constructor.setAccessible(true);
      final ConfigNodeDescriptor descriptor = constructor.newInstance();
      final ConfigNodeConfig conf = descriptor.getConf();
      final long updatedIntervalMs = conf.getTimeseriesDeletionExecutionIntervalMs() + 1;
      final TrimProperties properties = new TrimProperties();
      properties.setProperty(
          "timeseries_deletion_execution_interval_ms", String.valueOf(updatedIntervalMs));

      descriptor.loadHotModifiedProps(properties);

      assertEquals(updatedIntervalMs, conf.getTimeseriesDeletionExecutionIntervalMs());
    } finally {
      if (originalConf == null) {
        System.clearProperty(ConfigNodeConstant.CONFIGNODE_CONF);
      } else {
        System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, originalConf);
      }
    }
  }
}
