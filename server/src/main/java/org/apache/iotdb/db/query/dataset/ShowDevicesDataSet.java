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

package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_DEVICES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;

public class ShowDevicesDataSet extends ShowDataSet {

  private static final Path[] resourcePathsWithSg = {
    new PartialPath(COLUMN_DEVICES, false), new PartialPath(COLUMN_STORAGE_GROUP, false),
  };
  private static final TSDataType[] resourceTypesWithSg = {TSDataType.TEXT, TSDataType.TEXT};
  private static final Path[] resourcePaths = {new PartialPath(COLUMN_DEVICES, false)};
  private static final TSDataType[] resourceTypes = {TSDataType.TEXT};

  private boolean hasSgCol;

  public ShowDevicesDataSet(ShowDevicesPlan showDevicesPlan) throws MetadataException {
    super(
        showDevicesPlan.hasSgCol()
            ? Arrays.asList(resourcePathsWithSg)
            : Arrays.asList(resourcePaths),
        showDevicesPlan.hasSgCol()
            ? Arrays.asList(resourceTypesWithSg)
            : Arrays.asList(resourceTypes));
    hasSgCol = showDevicesPlan.hasSgCol();
    this.plan = showDevicesPlan;
    hasLimit = plan.hasLimit();
    getQueryDataSet();
  }

  @Override
  public List<RowRecord> getQueryDataSet() throws MetadataException {
    List<ShowDevicesResult> devicesList =
        IoTDB.metaManager.getMatchedDevices((ShowDevicesPlan) plan);
    List<RowRecord> records = new ArrayList<>();
    for (ShowDevicesResult result : devicesList) {
      RowRecord record = new RowRecord(0);
      updateRecord(record, result.getName());
      if (hasSgCol) {
        updateRecord(record, result.getSgName());
      }
      records.add(record);
      putRecord(record);
    }
    return records;
  }
}
