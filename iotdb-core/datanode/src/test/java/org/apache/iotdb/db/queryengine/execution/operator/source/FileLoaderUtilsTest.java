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

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileLoaderUtilsTest {

  @Test
  public void testShouldUseDeviceMetadataBatchScan() {
    int maxDegreeOfIndexNode = TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode();
    // a table wide enough to span more than one measurement leaf group
    int wideTableFieldCount = maxDegreeOfIndexNode * 2;
    int threshold = (int) Math.ceil(wideTableFieldCount * 0.6);

    // no field count is known (tree model, external tsfiles, or a table missing from the cache)
    assertFalse(FileLoaderUtils.shouldUseDeviceMetadataBatchScan(10, 0, false, false));

    // a device whose series fit in a single measurement leaf group gains nothing from batching
    assertFalse(
        FileLoaderUtils.shouldUseDeviceMetadataBatchScan(
            maxDegreeOfIndexNode, maxDegreeOfIndexNode, false, false));

    // below, at and above the threshold
    assertFalse(
        FileLoaderUtils.shouldUseDeviceMetadataBatchScan(
            threshold - 1, wideTableFieldCount, false, false));
    assertTrue(
        FileLoaderUtils.shouldUseDeviceMetadataBatchScan(
            threshold, wideTableFieldCount, false, false));
    assertTrue(
        FileLoaderUtils.shouldUseDeviceMetadataBatchScan(
            wideTableFieldCount, wideTableFieldCount, false, false));

    // a scan can reference measurements that no longer exist, so selected > total is possible
    assertTrue(
        FileLoaderUtils.shouldUseDeviceMetadataBatchScan(
            wideTableFieldCount + 1, wideTableFieldCount, false, false));

    // DEBUG queries keep the per-column cache's diagnostic logs
    assertFalse(
        FileLoaderUtils.shouldUseDeviceMetadataBatchScan(
            wideTableFieldCount, wideTableFieldCount, true, false));

    // offset-addressed external tsfiles cannot be resolved through the table metadata index
    assertFalse(
        FileLoaderUtils.shouldUseDeviceMetadataBatchScan(
            wideTableFieldCount, wideTableFieldCount, false, true));
  }
}
