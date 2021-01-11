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

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_DEVICES;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class ShowDevicesDataSet extends ShowDataSet {

  private static final Path[] resourcePaths = {new PartialPath(COLUMN_DEVICES, false)};
  private static final TSDataType[] resourceTypes = {TSDataType.TEXT};

  public ShowDevicesDataSet(ShowDevicesPlan showDevicesPlan) throws MetadataException {
    super(Arrays.asList(resourcePaths), Arrays.asList(resourceTypes));
    this.plan = showDevicesPlan;
    hasLimit = plan.hasLimit();
    getQueryDataSet();
  }

  public List<RowRecord> getQueryDataSet() throws MetadataException {
    Set<PartialPath> devicesSet = IoTDB.metaManager.getDevices((ShowDevicesPlan) plan);
    List<RowRecord> records = new ArrayList<>();
    for (PartialPath path : devicesSet) {
      RowRecord record = new RowRecord(0);
      updateRecord(record, path.getFullPath());
      records.add(record);
      putRecord(record);
    }
    return records;
  }
}
