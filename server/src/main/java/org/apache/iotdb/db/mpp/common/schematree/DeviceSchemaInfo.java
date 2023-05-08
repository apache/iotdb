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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DeviceSchemaInfo {

  private PartialPath devicePath;
  private boolean isAligned;
  private List<MeasurementSchemaInfo> measurementSchemaInfoList;

  private DeviceSchemaInfo() {}

  public DeviceSchemaInfo(
      PartialPath devicePath,
      boolean isAligned,
      List<MeasurementSchemaInfo> measurementSchemaInfoList) {
    this.devicePath = devicePath;
    this.isAligned = isAligned;
    this.measurementSchemaInfoList = measurementSchemaInfoList;
  }

  public PartialPath getDevicePath() {
    return devicePath;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public List<MeasurementSchema> getMeasurementSchemaList() {
    return measurementSchemaInfoList.stream()
        .map(
            measurementSchemaInfo ->
                measurementSchemaInfo == null ? null : measurementSchemaInfo.getSchema())
        .collect(Collectors.toList());
  }

  public List<MeasurementPath> getMeasurements(Set<String> measurements) {
    if (measurements.contains(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
      return measurementSchemaInfoList.stream()
          .map(
              measurementInfo -> {
                if (measurementInfo == null) {
                  return null;
                }
                MeasurementPath measurementPath =
                    new MeasurementPath(
                        devicePath.concatNode(measurementInfo.getName()),
                        measurementInfo.getSchema());
                if (measurementInfo.getAlias() != null) {
                  measurementPath.setMeasurementAlias(measurementInfo.getAlias());
                }
                measurementPath.setUnderAlignedEntity(isAligned);
                return measurementPath;
              })
          .collect(Collectors.toList());
    }
    List<MeasurementPath> measurementPaths = new ArrayList<>();
    for (MeasurementSchemaInfo measurementSchemaInfo : measurementSchemaInfoList) {
      MeasurementPath measurementPath =
          new MeasurementPath(
              devicePath.concatNode(measurementSchemaInfo.getName()),
              measurementSchemaInfo.getSchema());
      measurementPath.setUnderAlignedEntity(isAligned);
      if (measurements.contains(measurementSchemaInfo.getName())) {
        measurementPaths.add(measurementPath);
      } else if (measurementSchemaInfo.getAlias() != null
          && measurements.contains(measurementSchemaInfo.getAlias())) {
        measurementPath.setMeasurementAlias(measurementSchemaInfo.getAlias());
        measurementPaths.add(measurementPath);
      }
    }
    return measurementPaths;
  }
}
