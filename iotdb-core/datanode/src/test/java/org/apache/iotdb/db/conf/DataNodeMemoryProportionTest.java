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

package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.memory.MemoryConfig;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DataNodeMemoryProportionTest {

  @Parameterized.Parameters(name = "{index}: {0}={1}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> parameters = new ArrayList<>();
    for (String key :
        new String[] {
          "datanode_memory_proportion", "storage_query_schema_consensus_free_memory_proportion"
        }) {
      parameters.add(new Object[] {key, "3:3:1:1:2", 0.1});
      parameters.add(new Object[] {key, "3:3:1:1:0", 0.0});
      parameters.add(new Object[] {key, "3:3:1:1:1:1", 0.1});
      parameters.add(new Object[] {key, "3:3:1:1:2:0", 0.2});
    }
    return parameters;
  }

  private final String key;
  private final String proportion;
  private final double expectedPipeMemoryProportion;
  private String originalOffHeapMemory;

  public DataNodeMemoryProportionTest(
      String key, String proportion, double expectedPipeMemoryProportion) {
    this.key = key;
    this.proportion = proportion;
    this.expectedPipeMemoryProportion = expectedPipeMemoryProportion;
  }

  @Before
  public void setUp() {
    // Memory managers are reused by name, so each configuration needs a fresh hierarchy.
    MemoryConfig.global().clearAll();
    originalOffHeapMemory = System.getProperty("OFF_HEAP_MEMORY");
    System.setProperty("OFF_HEAP_MEMORY", "0");
  }

  @After
  public void tearDown() {
    MemoryConfig.global().clearAll();
    if (originalOffHeapMemory == null) {
      System.clearProperty("OFF_HEAP_MEMORY");
    } else {
      System.setProperty("OFF_HEAP_MEMORY", originalOffHeapMemory);
    }
  }

  @Test
  public void testPipeMemoryAllocation() {
    TrimProperties properties = new TrimProperties();
    properties.setProperty(key, proportion);
    DataNodeMemoryConfig memoryConfig = new DataNodeMemoryConfig();
    memoryConfig.init(
        properties, Integer.MAX_VALUE, LoggerFactory.getLogger(DataNodeMemoryProportionTest.class));

    long maxMemory = Runtime.getRuntime().maxMemory();
    long pipeMemory = memoryConfig.getPipeMemoryManager().getTotalMemorySizeInBytes();
    // Independent integer divisions can leave a few bytes to be split with free memory.
    assertEquals(maxMemory * expectedPipeMemoryProportion, pipeMemory, 2.0);
    assertTrue(
        memoryConfig.getStorageEngineMemoryManager().getTotalMemorySizeInBytes()
                + memoryConfig.getQueryEngineMemoryManager().getTotalMemorySizeInBytes()
                + memoryConfig.getSchemaEngineMemoryManager().getTotalMemorySizeInBytes()
                + memoryConfig.getConsensusMemoryManager().getTotalMemorySizeInBytes()
                + pipeMemory
            <= maxMemory);
  }
}
