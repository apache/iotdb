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

package org.apache.iotdb.db.queryengine.common.schematree;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;

import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;

public class DeviceSchemaInfo {

  private PartialPath devicePath;
  private boolean isAligned;
  private List<IMeasurementSchemaInfo> measurementSchemaInfoList;
  private int templateId = NON_TEMPLATE;

  private DeviceSchemaInfo() {}

  public DeviceSchemaInfo(
      PartialPath devicePath,
      boolean isAligned,
      int templateId,
      List<IMeasurementSchemaInfo> measurementSchemaInfoList) {
    this.devicePath = devicePath;
    this.isAligned = isAligned;
    this.templateId = templateId;
    this.measurementSchemaInfoList = measurementSchemaInfoList;
  }

  public PartialPath getDevicePath() {
    return devicePath;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public int getTemplateId() {
    return templateId;
  }

  public List<IMeasurementSchema> getMeasurementSchemaList() {
    return measurementSchemaInfoList.stream()
        .map(
            measurementSchemaInfo ->
                measurementSchemaInfo == null
                    ? null
                    : measurementSchemaInfo.getSchemaAsMeasurementSchema())
        .collect(Collectors.toList());
  }

  public List<IMeasurementSchemaInfo> getMeasurementSchemaInfoList() {
    return measurementSchemaInfoList;
  }

  public List<MeasurementPath> getMeasurementSchemaPathList() {
    return measurementSchemaInfoList.stream()
        .map(
            measurementSchemaInfo -> {
              if (measurementSchemaInfo == null) {
                return null;
              }
              MeasurementPath measurementPath =
                  new MeasurementPath(
                      devicePath.concatNode(measurementSchemaInfo.getName()),
                      measurementSchemaInfo.getSchema());
              if (measurementSchemaInfo.getAlias() != null) {
                measurementPath.setMeasurementAlias(measurementSchemaInfo.getAlias());
              }
              if (measurementSchemaInfo.getTagMap() != null) {
                measurementPath.setTagMap(measurementSchemaInfo.getTagMap());
              }
              measurementPath.setUnderAlignedEntity(isAligned);
              return measurementPath;
            })
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
    for (IMeasurementSchemaInfo measurementSchemaInfo : measurementSchemaInfoList) {
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
