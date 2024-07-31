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
import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/** factory to build device id according to configured algorithm */
public class DeviceIDFactory {
  private Function<String, IDeviceID> getDeviceIDFunction;

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

  private DeviceIDFactory() {
    getDeviceIDFunction = IDeviceID.Factory.DEFAULT_FACTORY::create;
  }

  // endregion

  /**
   * get device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return device id of the timeseries
   */
  public IDeviceID getDeviceID(final PartialPath devicePath) {
    return getDeviceIDFunction.apply(devicePath.getFullPath());
  }

  public static List<IDeviceID> convertRawDeviceIDs2PartitionKeys(
      final List<Object[]> deviceIdList) {
    final List<IDeviceID> tmpPartitionKeyList = new ArrayList<>();
    for (final Object[] rawId : deviceIdList) {
      final String[] partitionKey = new String[rawId.length];
      for (int i = 0; i < rawId.length; i++) {
        partitionKey[i] = (String) rawId[i];
      }
      tmpPartitionKeyList.add(IDeviceID.Factory.DEFAULT_FACTORY.create(partitionKey));
    }
    return tmpPartitionKeyList;
  }

  public static List<Object[]> truncateTailingNull(final List<Object[]> deviceIdList) {
    final List<Object[]> res = new ArrayList<>(deviceIdList.size());
    for (final Object[] device : deviceIdList) {
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
      // Use one "null" to indicate all "null"s
      if (lastNonNullIndex == -1) {
        res.add(new Object[] {null});
        continue;
      }
      res.add(
          lastNonNullIndex == device.length - 1
              ? device
              : Arrays.copyOf(device, lastNonNullIndex + 1));
    }
    return res;
  }

  /** reset id method */
  @TestOnly
  public void reset() {
    getDeviceIDFunction = StringArrayDeviceID::new;
  }
}
