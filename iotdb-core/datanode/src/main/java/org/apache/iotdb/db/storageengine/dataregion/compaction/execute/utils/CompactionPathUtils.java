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

import org.apache.tsfile.file.metadata.IDeviceID;

public class CompactionPathUtils {

  private CompactionPathUtils() {}

  public static PartialPath getPath(IDeviceID device, String measurement)
      throws IllegalPathException {
    if (device.isTableModel()) {
      String[] tableNameSegments =
          DataNodeDevicePathCache.getInstance().getPartialPath(device.getTableName()).getNodes();
      String[] nodes = new String[device.segmentNum() + tableNameSegments.length];
      System.arraycopy(tableNameSegments, 0, nodes, 0, tableNameSegments.length);
      for (int i = 0; i < device.segmentNum() - 1; i++) {
        nodes[i + tableNameSegments.length] = device.segment(i + 1).toString();
      }
      nodes[device.segmentNum() + tableNameSegments.length - 1] = measurement;
      MeasurementPath path = new MeasurementPath(nodes);
      path.setDevice(device);
      return path;
    } else {
      return DataNodeDevicePathCache.getInstance()
          .getPartialPath(device.toString())
          .concatAsMeasurementPath(measurement);
    }
  }
}
