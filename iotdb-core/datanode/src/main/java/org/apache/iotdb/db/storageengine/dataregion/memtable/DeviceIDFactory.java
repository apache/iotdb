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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.path.PartialPath;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** factory to build device id according to configured algorithm */
public class DeviceIDFactory {

  // region DeviceIDFactory Singleton
  private static class DeviceIDFactoryHolder {

    private DeviceIDFactoryHolder() {
      // allowed to do nothing
    }

    private static final DeviceIDFactory INSTANCE = new DeviceIDFactory();
  }

  /**
   * get instance
   *
   * @return instance of the factory
   */
  public static DeviceIDFactory getInstance() {
    return DeviceIDFactoryHolder.INSTANCE;
  }

  private DeviceIDFactory() {}

  // endregion

  /**
   * get device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return device id of the timeseries
   */
  public IDeviceID getDeviceID(final PartialPath devicePath) {
    return devicePath.getIDeviceID();
  }

  public static List<IDeviceID> convertRawDeviceIDs2PartitionKeys(
      final String tableName, final List<Object[]> deviceIdList) {
    final List<IDeviceID> tmpPartitionKeyList = new ArrayList<>();
    for (final Object[] rawId : deviceIdList) {
      final String[] partitionKey = new String[rawId.length + 1];
      partitionKey[0] = tableName;
      for (int i = 1; i <= rawId.length; i++) {
        partitionKey[i] = (String) rawId[i - 1];
      }
      tmpPartitionKeyList.add(IDeviceID.Factory.DEFAULT_FACTORY.create(partitionKey));
    }
    return tmpPartitionKeyList;
  }

  public static List<Object[]> truncateTailingNull(final List<Object[]> deviceIdList) {
    return deviceIdList.stream()
        .map(DeviceIDFactory::truncateTailingNull)
        .collect(Collectors.toList());
  }

  public static Object[] truncateTailingNull(final Object[] device) {
    if (device == null) {
      throw new IllegalArgumentException("DeviceID's length should be larger than 0.");
    }
    int lastNonNullIndex = -1;
    for (int i = device.length - 1; i >= 0; i--) {
      if (device[i] != null) {
        lastNonNullIndex = i;
        break;
      }
    }
    return lastNonNullIndex == device.length - 1
        ? device
        : Arrays.copyOf(device, lastNonNullIndex + 1);
  }
}
