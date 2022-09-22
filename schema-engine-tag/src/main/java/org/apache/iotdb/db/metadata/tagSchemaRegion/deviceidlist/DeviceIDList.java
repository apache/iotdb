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
package org.apache.iotdb.db.metadata.tagSchemaRegion.deviceidlist;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeviceIDList implements IDeviceIDList {

  private final List<IDeviceID> deviceIDS;

  private AppendOnlyDeviceIDListFileManager appendOnlyDeviceIDListFileManager;

  public DeviceIDList(String schemaDirPath) {
    deviceIDS = new ArrayList<>();
    appendOnlyDeviceIDListFileManager = new AppendOnlyDeviceIDListFileManager(schemaDirPath);
    recover();
  }

  public void recover() {
    appendOnlyDeviceIDListFileManager.recover(this);
  }

  @Override
  public void add(IDeviceID deviceID) {
    deviceIDS.add(deviceID);
    appendOnlyDeviceIDListFileManager.serialize(deviceID.toStringID());
  }

  @Override
  public IDeviceID get(int index) {
    return deviceIDS.get(index);
  }

  @Override
  public int size() {
    return deviceIDS.size();
  }

  @Override
  public List<IDeviceID> getAllDeviceIDS() {
    return new ArrayList<>(deviceIDS);
  }

  @TestOnly
  public void clear() throws IOException {
    appendOnlyDeviceIDListFileManager.close();
    deviceIDS.clear();
  }
}
