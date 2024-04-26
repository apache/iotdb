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

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.junit.Assert;
import org.junit.Test;

public class HashExecutorTest {

  private static final int SEED = 131;
  private static final String PATH_PREFIX = "root.db.d";
  private static final int TEST_SERIES_SLOT_NUM = 1000;
  private static final HashExecutor EXECUTOR = new HashExecutor(TEST_SERIES_SLOT_NUM);

  @Test
  public void hashExecutorCompatibleTest() {
    for (int suffix = 0; suffix < 1000; suffix++) {
      String device = PATH_PREFIX + suffix;
      IDeviceID deviceID = StringArrayDeviceID.getFACTORY().create(PATH_PREFIX + suffix);
      Assert.assertEquals(
          oldVersionGetSeriesPartitionSlot(device), EXECUTOR.getSeriesPartitionSlot(deviceID));
    }
  }

  private TSeriesPartitionSlot oldVersionGetSeriesPartitionSlot(String device) {
    int hash = 0;
    for (int i = 0; i < device.length(); i++) {
      hash = hash * SEED + (int) device.charAt(i);
    }
    hash &= Integer.MAX_VALUE;
    return new TSeriesPartitionSlot(hash % TEST_SERIES_SLOT_NUM);
  }
}
