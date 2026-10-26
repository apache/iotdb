/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.conf;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * The LOAD split partition cap bounds how many time partitions one source TsFile may span; a value
 * that rejects every LOAD, or that disables the bound, must not be accepted as configuration.
 */
public class LoadTsFileSplitPartitionMaxSizeConfigTest {

  @Test
  public void testZeroAndNegativeCapsAreRejected() {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final int original = config.getLoadTsFileSpiltPartitionMaxSize();
    try {
      for (final int rejected : new int[] {0, -1, Integer.MIN_VALUE}) {
        try {
          config.setLoadTsFileSpiltPartitionMaxSize(rejected);
          fail("expected " + rejected + " to be rejected as a LOAD split partition cap");
        } catch (final IllegalArgumentException expected) {
          // The cap of 0 would reject every LOAD that spans a time partition, which is every LOAD.
        }
      }
      assertEquals(
          "a rejected value must leave the configured cap untouched",
          original,
          config.getLoadTsFileSpiltPartitionMaxSize());

      config.setLoadTsFileSpiltPartitionMaxSize(1);
      assertEquals(1, config.getLoadTsFileSpiltPartitionMaxSize());
      config.setLoadTsFileSpiltPartitionMaxSize(original);
      assertEquals(original, config.getLoadTsFileSpiltPartitionMaxSize());
    } finally {
      config.setLoadTsFileSpiltPartitionMaxSize(original);
    }
  }
}
