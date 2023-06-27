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
    getDeviceIDFunction = PlainDeviceID::new;
  }
  // endregion

  /**
   * get device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return device id of the timeseries
   */
  public IDeviceID getDeviceID(PartialPath devicePath) {
    return getDeviceIDFunction.apply(devicePath.toString());
  }

  /**
   * get device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return device id of the timeseries
   */
  public IDeviceID getDeviceID(String devicePath) {
    return getDeviceIDFunction.apply(devicePath);
  }

  /** reset id method */
  @TestOnly
  public void reset() {
    getDeviceIDFunction = PlainDeviceID::new;
  }
}
