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

import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.memory.MemoryConfig;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConfigNodeMemoryConfigTest {

  private static final String ON_HEAP_MEMORY_MANAGER_NAME = "ConfigNodeOnHeap";

  @Before
  public void setUp() {
    MemoryConfig.global().releaseChildMemoryManager(ON_HEAP_MEMORY_MANAGER_NAME);
  }

  @After
  public void tearDown() {
    MemoryConfig.global().releaseChildMemoryManager(ON_HEAP_MEMORY_MANAGER_NAME);
  }

  @Test
  public void testConfigNodeMemoryFrameworkOnlyCreatesPipeMemoryManager() {
    final TrimProperties properties = new TrimProperties();
    properties.setProperty("confignode_memory_proportion", "1:3");

    final ConfigNodeMemoryConfig memoryConfig = new ConfigNodeMemoryConfig();
    memoryConfig.init(properties);

    Assert.assertEquals(
        Runtime.getRuntime().maxMemory() / 4,
        memoryConfig.getPipeMemoryManager().getTotalMemorySizeInBytes());
    Assert.assertNotNull(
        memoryConfig
            .getOnHeapMemoryManager()
            .getMemoryManager(ConfigNodeMemoryConfig.PIPE_MEMORY_MANAGER_NAME));
    Assert.assertNull(memoryConfig.getOnHeapMemoryManager().getMemoryManager("Free"));
  }
}
