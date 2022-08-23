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

package org.apache.iotdb.db.metadata.idtable.entry;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.idtable.deviceID.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.deviceID.PlainDeviceID;
import org.apache.iotdb.db.metadata.idtable.deviceID.SHA256DeviceID;
import org.apache.iotdb.db.metadata.idtable.deviceID.StandAloneAutoIncDeviceID;

import java.util.function.Function;

/** factory to build device id according to configured algorithm */
public class DeviceIDFactory {
  Function<String, IDeviceID> getDeviceIDFunction;

  Function<String, IDeviceID> getDeviceIDWithAutoCreateFunction;

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
        return;
      } else if (IoTDBDescriptor.getInstance()
          .getConfig()
          .getDeviceIDTransformationMethod()
          .equals("AutoIncrement_INT")) {
        getDeviceIDFunction = StandAloneAutoIncDeviceID::getDeviceID;
        getDeviceIDWithAutoCreateFunction = StandAloneAutoIncDeviceID::getDeviceIDWithAutoCreate;
        return;
      }
    }
    getDeviceIDFunction = PlainDeviceID::new;
    getDeviceIDWithAutoCreateFunction = PlainDeviceID::new;
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

  /**
   * get and set device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return device id of the timeseries
   */
  public IDeviceID getDeviceIDWithAutoCreate(PartialPath devicePath) {
    return getDeviceIDWithAutoCreateFunction.apply(devicePath.toString());
  }

  /**
   * get and set device id by full path
   *
   * @param devicePath device path of the timeseries
   * @return device id of the timeseries
   */
  public IDeviceID getDeviceIDWithAutoCreate(String devicePath) {
    return getDeviceIDWithAutoCreateFunction.apply(devicePath);
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
          .equals("AutoIncrement_INT")) {
        getDeviceIDFunction = StandAloneAutoIncDeviceID::getDeviceID;
        getDeviceIDWithAutoCreateFunction = StandAloneAutoIncDeviceID::getDeviceIDWithAutoCreate;
        StandAloneAutoIncDeviceID.reset();
        return;
      }
    }
    getDeviceIDFunction = PlainDeviceID::new;
    getDeviceIDWithAutoCreateFunction = PlainDeviceID::new;
  }

  public Class getDeviceIDClass() {
    if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
      if (IoTDBDescriptor.getInstance()
          .getConfig()
          .getDeviceIDTransformationMethod()
          .equals("SHA256")) {
        return SHA256DeviceID.class;
      } else if (IoTDBDescriptor.getInstance()
          .getConfig()
          .getDeviceIDTransformationMethod()
          .equals("AutoIncrement_INT")) {
        return StandAloneAutoIncDeviceID.class;
      }
    }
    return PlainDeviceID.class;
  }
}
