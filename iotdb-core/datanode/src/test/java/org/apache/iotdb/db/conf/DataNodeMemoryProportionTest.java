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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
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
  private IoTDBConfig config;
  private long[] originalMemoryAllocations;

  public DataNodeMemoryProportionTest(
      String key, String proportion, double expectedPipeMemoryProportion) {
    this.key = key;
    this.proportion = proportion;
    this.expectedPipeMemoryProportion = expectedPipeMemoryProportion;
  }

  @Before
  public void setUp() {
    config = IoTDBDescriptor.getInstance().getConfig();
    originalMemoryAllocations =
        new long[] {
          config.getAllocateMemoryForStorageEngine(),
          config.getAllocateMemoryForRead(),
          config.getAllocateMemoryForSchema(),
          config.getAllocateMemoryForConsensus(),
          config.getAllocateMemoryForPipe(),
          config.getAllocateMemoryForBloomFilterCache(),
          config.getAllocateMemoryForChunkCache(),
          config.getAllocateMemoryForTimeSeriesMetaDataCache(),
          config.getAllocateMemoryForCoordinator(),
          config.getAllocateMemoryForOperators(),
          config.getAllocateMemoryForDataExchange(),
          config.getAllocateMemoryForTimeIndex(),
          config.getMaxBytesPerFragmentInstance(),
          config.getAllocateMemoryForSchemaRegion(),
          config.getAllocateMemoryForSchemaCache(),
          config.getAllocateMemoryForPartitionCache()
        };
  }

  @After
  public void tearDown() {
    config.setAllocateMemoryForStorageEngine(originalMemoryAllocations[0]);
    config.setAllocateMemoryForRead(originalMemoryAllocations[1]);
    config.setAllocateMemoryForSchema(originalMemoryAllocations[2]);
    config.setAllocateMemoryForConsensus(originalMemoryAllocations[3]);
    config.setAllocateMemoryForPipe(originalMemoryAllocations[4]);
    config.setAllocateMemoryForBloomFilterCache(originalMemoryAllocations[5]);
    config.setAllocateMemoryForChunkCache(originalMemoryAllocations[6]);
    config.setAllocateMemoryForTimeSeriesMetaDataCache(originalMemoryAllocations[7]);
    config.setAllocateMemoryForCoordinator(originalMemoryAllocations[8]);
    config.setAllocateMemoryForOperators(originalMemoryAllocations[9]);
    config.setAllocateMemoryForDataExchange(originalMemoryAllocations[10]);
    config.setAllocateMemoryForTimeIndex(originalMemoryAllocations[11]);
    config.setMaxBytesPerFragmentInstance(originalMemoryAllocations[12]);
    config.setAllocateMemoryForSchemaRegion(originalMemoryAllocations[13]);
    config.setAllocateMemoryForSchemaCache(originalMemoryAllocations[14]);
    config.setAllocateMemoryForPartitionCache(originalMemoryAllocations[15]);
  }

  @Test
  public void testPipeMemoryAllocation() throws Exception {
    TrimProperties properties = new TrimProperties();
    properties.setProperty(key, proportion);

    // Keep this test focused on memory allocation without reloading unrelated singleton settings.
    Method initMemoryAllocate =
        IoTDBDescriptor.class.getDeclaredMethod("initMemoryAllocate", TrimProperties.class);
    initMemoryAllocate.setAccessible(true);
    initMemoryAllocate.invoke(IoTDBDescriptor.getInstance(), properties);

    long maxMemory = Runtime.getRuntime().maxMemory();
    long pipeMemory = config.getAllocateMemoryForPipe();
    assertEquals(maxMemory * expectedPipeMemoryProportion, pipeMemory, 2.0);
    assertTrue(
        config.getAllocateMemoryForStorageEngine()
                + config.getAllocateMemoryForRead()
                + config.getAllocateMemoryForSchema()
                + config.getAllocateMemoryForConsensus()
                + pipeMemory
            <= maxMemory);
  }
}
