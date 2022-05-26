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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DeviceSchemaInfo {

  private final PartialPath devicePath;
  private final boolean isAligned;
  private final List<SchemaMeasurementNode> measurementNodeList;

  public DeviceSchemaInfo(
      PartialPath devicePath, boolean isAligned, List<SchemaMeasurementNode> measurementNodeList) {
    this.devicePath = devicePath;
    this.isAligned = isAligned;
    this.measurementNodeList = measurementNodeList;
  }

  public PartialPath getDevicePath() {
    return devicePath;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public List<MeasurementSchema> getMeasurementSchemaList() {
    return measurementNodeList.stream()
        .map(measurementNode -> measurementNode == null ? null : measurementNode.getSchema())
        .collect(Collectors.toList());
  }

  public List<MeasurementPath> getMeasurements(Set<String> measurements) {
    if (measurements.contains(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
      return measurementNodeList.stream()
          .map(
              measurementNode -> {
                if (measurementNode == null) {
                  return null;
                }
                MeasurementPath measurementPath =
                    new MeasurementPath(
                        devicePath.concatNode(measurementNode.getName()),
                        measurementNode.getSchema());
                if (measurementNode.getAlias() != null) {
                  measurementPath.setMeasurementAlias(measurementNode.getAlias());
                }
                measurementPath.setUnderAlignedEntity(isAligned);
                return measurementPath;
              })
          .collect(Collectors.toList());
    }
    List<MeasurementPath> measurementPaths = new ArrayList<>();
    for (SchemaMeasurementNode measurementNode : measurementNodeList) {
      MeasurementPath measurementPath =
          new MeasurementPath(
              devicePath.concatNode(measurementNode.getName()), measurementNode.getSchema());
      measurementPath.setUnderAlignedEntity(isAligned);
      if (measurements.contains(measurementNode.getName())) {
        measurementPaths.add(measurementPath);
      } else if (measurementNode.getAlias() != null
          && measurements.contains(measurementNode.getAlias())) {
        measurementPath.setMeasurementAlias(measurementNode.getAlias());
        measurementPaths.add(measurementPath);
      }
    }
    return measurementPaths;
  }

  public MeasurementPath getPathByMeasurement(String measurementName) {
    for (SchemaMeasurementNode measurementNode : measurementNodeList) {
      MeasurementPath measurementPath =
          new MeasurementPath(
              devicePath.concatNode(measurementNode.getName()), measurementNode.getSchema());
      measurementPath.setUnderAlignedEntity(isAligned);
      if (measurementNode.getName().equals(measurementName)) {
        return measurementPath;
      } else if (measurementNode.getAlias() != null
          && measurementNode.getAlias().equals(measurementName)) {
        measurementPath.setMeasurementAlias(measurementNode.getAlias());
        return measurementPath;
      }
    }
    throw new SemanticException(
        String.format(
            "ALIGN BY DEVICE: measurement '%s' does not exist in device '%s'",
            measurementName, getDevicePath()));
  }
}
