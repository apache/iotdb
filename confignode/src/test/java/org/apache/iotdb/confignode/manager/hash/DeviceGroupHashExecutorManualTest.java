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
package org.apache.iotdb.confignode.manager.hash;

import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * This is a not active test class, which can be used for general index testing when there is a new
 * DeviceGroup hash algorithm
 */
public class DeviceGroupHashExecutorManualTest {

  private static final int deviceGroupCount = 10_000;
  private static final String sg = "root.SGGroup.";
  private static final int batchCount = 10_000;
  private static final int batchSize = 10_000;
  private static final String chars =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";

  private List<String> genBatchDevices() {
    Random random = new Random();
    List<String> devices = new ArrayList<>();
    int fatherLength = random.nextInt(10) + 10;
    int deviceLength = random.nextInt(5) + 5;

    for (int i = 0; i < batchSize; i++) {
      StringBuilder curDevice = new StringBuilder(sg);
      for (int j = 0; j < fatherLength; j++) {
        curDevice.append(chars.charAt(random.nextInt(chars.length())));
      }
      curDevice.append('.');
      for (int k = 0; k < deviceLength; k++) {
        curDevice.append(chars.charAt(random.nextInt(chars.length())));
      }
      devices.add(curDevice.toString());
    }
    return devices;
  }

  public void GeneralIndexTest() throws IOException {
    PartitionManager manager = new PartitionManager(new ConfigManager(), new PartitionInfo());
    int[] bucket = new int[deviceGroupCount];
    Arrays.fill(bucket, 0);

    long totalTime = 0;
    for (int i = 0; i < batchCount; i++) {
      List<String> devices = genBatchDevices();
      totalTime -= System.currentTimeMillis();
      for (String device : devices) {
        bucket[manager.getSeriesPartitionSlot(device).getSlotId()] += 1;
      }
      totalTime += System.currentTimeMillis();
    }

    Arrays.sort(bucket);
    int firstNotNull = 0;
    for (; ; firstNotNull++) {
      if (bucket[firstNotNull] > 0) {
        break;
      }
    }
    System.out.println("Empty DeviceGroup count: " + firstNotNull);
    System.out.println("Minimum DeviceGroup size: " + bucket[firstNotNull]);
    System.out.println("Maximal DeviceGroup size: " + bucket[deviceGroupCount - 1]);
    System.out.println(
        "Average size of nonempty DeviceGroup: "
            + (double) (batchCount * batchSize) / (double) (deviceGroupCount - firstNotNull));
    System.out.println(
        "Median size of nonempty DeviceGroup: " + bucket[(deviceGroupCount - firstNotNull) / 2]);
    System.out.println("Total time-consuming: " + (double) totalTime / 1000.0 + "s");
  }
}
