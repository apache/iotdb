/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import static org.junit.Assert.assertThrows;

public class LoadStatisticsPublisherConfigTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDefaultsAndPositiveSize() throws Exception {
    ConfigNodeConfig config = new ConfigNodeConfig();
    assertEquals(5, config.getLoadStatisticsPublisherThreadCount());
    assertEquals(
        "5",
        ConfigurationFileUtils.getConfigurationDefaultValue(
            "cn_load_statistics_publisher_thread_count"));
    for (int invalid : new int[] {0, -1}) {
      assertThrows(
          IllegalArgumentException.class,
          () -> config.setLoadStatisticsPublisherThreadCount(invalid));
    }
    assertEquals(5, config.getLoadStatisticsPublisherThreadCount());
  }

  @Test
  public void testStartupOverrideIsRestartOnly() throws Exception {
    String originalConf = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF);
    File confDir = temporaryFolder.newFolder();
    Files.writeString(
        confDir.toPath().resolve(CommonConfig.SYSTEM_CONFIG_NAME),
        "cn_seed_config_node=127.0.0.1:10710\ncn_load_statistics_publisher_thread_count=2\n",
        StandardCharsets.UTF_8);
    System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, confDir.getAbsolutePath());
    try {
      Constructor<ConfigNodeDescriptor> constructor =
          ConfigNodeDescriptor.class.getDeclaredConstructor();
      constructor.setAccessible(true);
      ConfigNodeDescriptor descriptor = constructor.newInstance();
      assertEquals(2, descriptor.getConf().getLoadStatisticsPublisherThreadCount());

      TrimProperties properties = new TrimProperties();
      properties.setProperty("cn_load_statistics_publisher_thread_count", "3");
      descriptor.loadHotModifiedProps(properties);
      assertEquals(2, descriptor.getConf().getLoadStatisticsPublisherThreadCount());
    } finally {
      if (originalConf == null) {
        System.clearProperty(ConfigNodeConstant.CONFIGNODE_CONF);
      } else {
        System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, originalConf);
      }
    }
  }
}
