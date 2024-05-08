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
package org.apache.iotdb.db.metadata.tagSchemaRegion.idtable;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.idtable.IDTableHashmapImpl;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * an idTable implementation，manager timeSeries, and use a deviceID list manager device id -> INT32
 * id
 */
public class IDTableWithDeviceIDListImpl extends IDTableHashmapImpl {

  // use a deviceID list manager device id -> INT32 id
  private List<IDeviceID> deviceIDS;

  public IDTableWithDeviceIDListImpl(File storageGroupDir) {
    super(storageGroupDir);
    if (deviceIDS == null) {
      deviceIDS = new ArrayList<>();
    }
  }

  /**
   * Whether device entry already exists
   *
   * @param devicePath device path
   * @return false if the device entry already exists, otherwise return true
   */
  private boolean deviceEntryNotExist(String devicePath) {
    DeviceEntry deviceEntry = getDeviceEntry(devicePath);
    return deviceEntry == null;
  }
  /**
   * create aligned timeseries
   *
   * @param plan create aligned timeseries plan
   * @throws MetadataException
   */
  @Override
  public synchronized void createAlignedTimeseries(ICreateAlignedTimeSeriesPlan plan)
      throws MetadataException {
    String devicePath = plan.getDevicePath().getFullPath();
    // This device path is created for the first time, append it to deviceIdList
    if (deviceEntryNotExist(devicePath)) {
      IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath);
      deviceIDS.add(deviceID);
    }
    super.createAlignedTimeseries(plan);
  }

  /**
   * create timeseries
   *
   * @param plan create timeseries plan
   * @throws MetadataException
   */
  @Override
  public synchronized void createTimeseries(ICreateTimeSeriesPlan plan) throws MetadataException {
    String devicePath = plan.getPath().getDevicePath().getFullPath();
    // This device path is created for the first time, append it to deviceIdList
    if (deviceEntryNotExist(devicePath)) {
      IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath);
      deviceIDS.add(deviceID);
    }
    super.createTimeseries(plan);
  }

  /**
   * put schema entry to id table, currently used in recover
   *
   * @param devicePath device path (can be device id formed path)
   * @param measurement measurement name
   * @param schemaEntry schema entry to put
   * @param isAligned is the device aligned
   */
  @Override
  public void putSchemaEntry(
      String devicePath, String measurement, SchemaEntry schemaEntry, boolean isAligned)
      throws MetadataException {
    if (deviceIDS == null) {
      deviceIDS = new ArrayList<>();
    }
    // This device path is created for the first time, append it to deviceIdList
    if (deviceEntryNotExist(devicePath)) {
      IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath);
      deviceIDS.add(deviceID);
    }
    super.putSchemaEntry(devicePath, measurement, schemaEntry, isAligned);
  }

  @Override
  @TestOnly
  public void clear() throws IOException {
    super.clear();
    deviceIDS = null;
  }

  public synchronized IDeviceID get(int index) {
    return deviceIDS.get(index);
  }

  public int size() {
    return deviceIDS.size();
  }

  public List<IDeviceID> getAllDeviceIDS() {
    return new ArrayList<>(deviceIDS);
  }
}
