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

package org.apache.iotdb.commons.partition.executor;

import org.apache.iotdb.commons.partition.executor.hash.APHashExecutor;
import org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor;
import org.apache.iotdb.commons.partition.executor.hash.JSHashExecutor;
import org.apache.iotdb.commons.partition.executor.hash.SDBMHashExecutor;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Assert;
import org.junit.Test;

public class HashExecutorTest {
  private static final String PATH_PREFIX = "root.db.g1.d";
  private static final int TEST_SERIES_SLOT_NUM = 1000;
  private static final BKDRHashExecutor BKDR_HASH_EXECUTOR =
      new BKDRHashExecutor(TEST_SERIES_SLOT_NUM);

  private static final APHashExecutor AP_HASH_EXECUTOR = new APHashExecutor(TEST_SERIES_SLOT_NUM);

  private static final JSHashExecutor JS_HASH_EXECUTOR = new JSHashExecutor(TEST_SERIES_SLOT_NUM);

  private static final SDBMHashExecutor SDBM_HASH_EXECUTOR =
      new SDBMHashExecutor(TEST_SERIES_SLOT_NUM);

  @Test
  public void hashExecutorCompatibleTest() {
    for (int suffix = 0; suffix < 1000; suffix++) {
      String device = PATH_PREFIX + suffix;
      IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(PATH_PREFIX + suffix);
      Assert.assertEquals(
          BKDR_HASH_EXECUTOR.getSeriesPartitionSlot(device),
          BKDR_HASH_EXECUTOR.getSeriesPartitionSlot(deviceID));
      Assert.assertEquals(
          AP_HASH_EXECUTOR.getSeriesPartitionSlot(device),
          AP_HASH_EXECUTOR.getSeriesPartitionSlot(deviceID));
      Assert.assertEquals(
          JS_HASH_EXECUTOR.getSeriesPartitionSlot(device),
          JS_HASH_EXECUTOR.getSeriesPartitionSlot(deviceID));
      Assert.assertEquals(
          SDBM_HASH_EXECUTOR.getSeriesPartitionSlot(device),
          SDBM_HASH_EXECUTOR.getSeriesPartitionSlot(deviceID));
    }
  }
}
