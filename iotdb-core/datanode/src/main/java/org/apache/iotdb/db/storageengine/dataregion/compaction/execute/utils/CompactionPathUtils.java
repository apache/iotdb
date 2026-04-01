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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.IDeviceID;

public class CompactionPathUtils {

  private CompactionPathUtils() {}

  public static MeasurementPath getPath(final IDeviceID device, final String measurement)
      throws IllegalPathException {
    return getPath(device).concatAsMeasurementPath(measurement);
  }

  public static PartialPath getPath(final IDeviceID device) throws IllegalPathException {
    if (device.isTableModel()) {
      final String[] nodes = new String[device.segmentNum() + 1];
      nodes[0] = device.getTableName();
      for (int i = 0; i < device.segmentNum() - 1; i++) {
        nodes[i + 1] = device.segment(i + 1) == null ? null : device.segment(i + 1).toString();
      }
      return new PartialPath(nodes);
    } else {
      final String deviceId = device.toString();
      return deviceId.contains(TsFileConstant.BACK_QUOTE_STRING)
          ? DataNodeDevicePathCache.getInstance().getPartialPath(deviceId)
          : new PartialPath(deviceId.split(TsFileConstant.PATH_SEPARATER_NO_REGEX));
    }
  }
}
