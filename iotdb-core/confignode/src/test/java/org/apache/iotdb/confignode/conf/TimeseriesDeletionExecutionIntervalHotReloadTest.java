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
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TimeseriesDeletionExecutionIntervalHotReloadTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testConfigurationCanBeAppliedWithoutRestart() throws Exception {
    final Map<String, ConfigurationFileUtils.DefaultConfigurationItem> items =
        ConfigurationFileUtils.getConfigurationItemsFromTemplate(false);
    final ConfigurationFileUtils.DefaultConfigurationItem intervalItem =
        items.get("timeseries_deletion_execution_interval_ms");
    final ConfigurationFileUtils.DefaultConfigurationItem queueCapacityItem =
        items.get("timeseries_deletion_queue_capacity");

    assertNotNull(intervalItem);
    assertEquals(ConfigurationFileUtils.EffectiveModeType.HOT_RELOAD, intervalItem.effectiveMode);
    assertNotNull(queueCapacityItem);
    assertEquals(
        ConfigurationFileUtils.EffectiveModeType.HOT_RELOAD, queueCapacityItem.effectiveMode);
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
      final int originalQueueCapacity = conf.getTimeseriesDeletionQueueCapacity();
      final long updatedIntervalMs = conf.getTimeseriesDeletionExecutionIntervalMs() + 1;
      final int updatedQueueCapacity = conf.getTimeseriesDeletionQueueCapacity() + 1;
      final TrimProperties invalidProperties = new TrimProperties();
      invalidProperties.setProperty("timeseries_deletion_queue_capacity", "0");
      descriptor.loadHotModifiedProps(invalidProperties);
      assertEquals(originalQueueCapacity, conf.getTimeseriesDeletionQueueCapacity());

      final TrimProperties properties = new TrimProperties();
      properties.setProperty(
          "timeseries_deletion_execution_interval_ms", String.valueOf(updatedIntervalMs));
      properties.setProperty(
          "timeseries_deletion_queue_capacity", String.valueOf(updatedQueueCapacity));

      descriptor.loadHotModifiedProps(properties);

      assertEquals(updatedIntervalMs, conf.getTimeseriesDeletionExecutionIntervalMs());
      assertEquals(updatedQueueCapacity, conf.getTimeseriesDeletionQueueCapacity());
    } finally {
      if (originalConf == null) {
        System.clearProperty(ConfigNodeConstant.CONFIGNODE_CONF);
      } else {
        System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, originalConf);
      }
    }
  }
}
