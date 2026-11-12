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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileLoaderUtilsTest {

  @Test
  public void testShouldUseDeviceMetadataBatchScan() {
    // no field count is known (tree model, external tsfiles, or a table missing from the cache)
    assertFalse(FileLoaderUtils.shouldUseDeviceMetadataBatchScan(10, 0));

    // a single-column device gains nothing from a batch read
    assertFalse(FileLoaderUtils.shouldUseDeviceMetadataBatchScan(1, 1));

    // below, at and above the threshold
    assertFalse(FileLoaderUtils.shouldUseDeviceMetadataBatchScan(5, 10));
    assertTrue(FileLoaderUtils.shouldUseDeviceMetadataBatchScan(6, 10));
    assertTrue(FileLoaderUtils.shouldUseDeviceMetadataBatchScan(10, 10));

    // a scan can reference measurements that no longer exist, so selected > total is possible
    assertTrue(FileLoaderUtils.shouldUseDeviceMetadataBatchScan(11, 10));
  }
}
