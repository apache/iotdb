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
package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.confignode.manager.partition.RegionGroupExtensionPolicy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RegionGroupExtensionPolicyConfigTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testParsePolicies() throws Exception {
    for (RegionGroupExtensionPolicy policy : RegionGroupExtensionPolicy.values()) {
      assertEquals(policy, RegionGroupExtensionPolicy.parse(policy.getPolicy()));
    }
    assertEquals(
        RegionGroupExtensionPolicy.PROACTIVE, RegionGroupExtensionPolicy.parse("PROACTIVE"));
    assertThrows(IOException.class, () -> RegionGroupExtensionPolicy.parse("UNKNOWN"));
  }

  @Test
  public void testProactiveStartupAndHotReloadForBothTypes() throws Exception {
    String originalConf = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF);
    File confDir = temporaryFolder.newFolder();
    Files.writeString(
        confDir.toPath().resolve(CommonConfig.SYSTEM_CONFIG_NAME),
        "cn_seed_config_node=127.0.0.1:10710\n"
            + "schema_region_group_extension_policy=PROACTIVE\n"
            + "data_region_group_extension_policy=PROACTIVE\n",
        StandardCharsets.UTF_8);
    System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, confDir.getAbsolutePath());
    try {
      Constructor<ConfigNodeDescriptor> constructor =
          ConfigNodeDescriptor.class.getDeclaredConstructor();
      constructor.setAccessible(true);
      ConfigNodeDescriptor descriptor = constructor.newInstance();
      ConfigNodeConfig conf = descriptor.getConf();
      assertEquals(
          RegionGroupExtensionPolicy.PROACTIVE, conf.getSchemaRegionGroupExtensionPolicy());
      assertEquals(RegionGroupExtensionPolicy.PROACTIVE, conf.getDataRegionGroupExtensionPolicy());

      TrimProperties properties = new TrimProperties();
      properties.setProperty("schema_region_group_extension_policy", "CUSTOM");
      properties.setProperty("data_region_group_extension_policy", "AUTO");
      descriptor.loadHotModifiedProps(properties);
      assertEquals(RegionGroupExtensionPolicy.CUSTOM, conf.getSchemaRegionGroupExtensionPolicy());
      assertEquals(RegionGroupExtensionPolicy.AUTO, conf.getDataRegionGroupExtensionPolicy());

      properties.setProperty("schema_region_group_extension_policy", "PROACTIVE");
      properties.setProperty("data_region_group_extension_policy", "PROACTIVE");
      descriptor.loadHotModifiedProps(properties);
      assertEquals(
          RegionGroupExtensionPolicy.PROACTIVE, conf.getSchemaRegionGroupExtensionPolicy());
      assertEquals(RegionGroupExtensionPolicy.PROACTIVE, conf.getDataRegionGroupExtensionPolicy());

      TrimProperties schemaOnly = new TrimProperties();
      schemaOnly.setProperty("schema_region_group_extension_policy", "AUTO");
      descriptor.loadHotModifiedProps(schemaOnly);
      assertEquals(RegionGroupExtensionPolicy.AUTO, conf.getSchemaRegionGroupExtensionPolicy());
      assertEquals(RegionGroupExtensionPolicy.PROACTIVE, conf.getDataRegionGroupExtensionPolicy());

      TrimProperties dataOnly = new TrimProperties();
      dataOnly.setProperty("data_region_group_extension_policy", "CUSTOM");
      descriptor.loadHotModifiedProps(dataOnly);
      assertEquals(RegionGroupExtensionPolicy.AUTO, conf.getSchemaRegionGroupExtensionPolicy());
      assertEquals(RegionGroupExtensionPolicy.CUSTOM, conf.getDataRegionGroupExtensionPolicy());
    } finally {
      if (originalConf == null) {
        System.clearProperty(ConfigNodeConstant.CONFIGNODE_CONF);
      } else {
        System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, originalConf);
      }
    }
  }
}
