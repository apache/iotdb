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

package org.apache.iotdb.db.metadata.idtable.deviceID;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.util.function.Function;

/** factory to build device id according to configured algorithm */
public class DeviceIDFactory {

  /** used to obtain a IDeviceID instance in the query operation */
  Function<String, IDeviceID> getDeviceIDFunction;

  /** used to obtain a IDeviceID instance in the insert operation */
  Function<String, IDeviceID> getDeviceIDWithAutoCreateFunction;

  /** used to obtain a IDeviceID instance in the system restart */
  Function<String[], IDeviceID> getDeviceIDWithRecoverFunction;

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
    if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
      if (IoTDBDescriptor.getInstance()
          .getConfig()
          .getDeviceIDTransformationMethod()
          .equals("SHA256")) {
        getDeviceIDFunction = SHA256DeviceID::new;
        getDeviceIDWithAutoCreateFunction = SHA256DeviceID::new;
        getDeviceIDWithRecoverFunction = null;
        return;
      } else if (IoTDBDescriptor.getInstance()
          .getConfig()
          .getDeviceIDTransformationMethod()
          .equals("AutoIncrement")) {
        getDeviceIDFunction = StandAloneAutoIncDeviceID::getDeviceID;
        getDeviceIDWithAutoCreateFunction = StandAloneAutoIncDeviceID::getDeviceIDWithAutoCreate;
        getDeviceIDWithRecoverFunction = StandAloneAutoIncDeviceID::getDeviceIDWithRecover;
        return;
      }
    }
    getDeviceIDFunction = PlainDeviceID::new;
    getDeviceIDWithAutoCreateFunction = PlainDeviceID::new;
    getDeviceIDWithRecoverFunction = null;
  }
  // endregion

  /**
   * get device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return a IDeviceID instance of the device path
   */
  public IDeviceID getDeviceID(PartialPath devicePath) {
    return getDeviceIDFunction.apply(devicePath.toString());
  }

  /**
   * get device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return a IDeviceID instance of the device path
   */
  public IDeviceID getDeviceID(String devicePath) {
    return getDeviceIDFunction.apply(devicePath);
  }

  /**
   * get and set device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return a IDeviceID instance of the device path
   */
  public IDeviceID getDeviceIDWithAutoCreate(PartialPath devicePath) {
    return getDeviceIDWithAutoCreateFunction.apply(devicePath.toString());
  }

  /**
   * get and set device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return a IDeviceID instance of the device path
   */
  public IDeviceID getDeviceIDWithAutoCreate(String devicePath) {
    return getDeviceIDWithAutoCreateFunction.apply(devicePath);
  }

  /**
   * get a device id, only for recover
   *
   * @param deviceID device id
   * @param devicePath device path of the timeseries
   * @return a IDeviceID instance of the device path
   */
  public IDeviceID getDeviceIDWithRecover(String deviceID, String devicePath) {
    if (getDeviceIDWithRecoverFunction == null) {
      return getDeviceID(devicePath);
    } else {
      return getDeviceIDWithRecoverFunction.apply(new String[] {deviceID, devicePath});
    }
  }

  /** reset id method */
  @TestOnly
  public void reset() {
    if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
      if (IoTDBDescriptor.getInstance()
          .getConfig()
          .getDeviceIDTransformationMethod()
          .equals("SHA256")) {
        getDeviceIDFunction = SHA256DeviceID::new;
        getDeviceIDWithAutoCreateFunction = SHA256DeviceID::new;
        return;
      } else if (IoTDBDescriptor.getInstance()
          .getConfig()
          .getDeviceIDTransformationMethod()
          .equals("AutoIncrement")) {
        getDeviceIDFunction = StandAloneAutoIncDeviceID::getDeviceID;
        getDeviceIDWithAutoCreateFunction = StandAloneAutoIncDeviceID::getDeviceIDWithAutoCreate;
        getDeviceIDWithRecoverFunction = StandAloneAutoIncDeviceID::getDeviceIDWithRecover;
        StandAloneAutoIncDeviceID.reset();
        return;
      }
    }
    getDeviceIDFunction = PlainDeviceID::new;
    getDeviceIDWithAutoCreateFunction = PlainDeviceID::new;
  }

  /** clear device id state */
  @TestOnly
  public void clear() {
    if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
      if (IoTDBDescriptor.getInstance()
          .getConfig()
          .getDeviceIDTransformationMethod()
          .equals("AutoIncrement")) {
        StandAloneAutoIncDeviceID.clear();
      }
    }
  }
}
