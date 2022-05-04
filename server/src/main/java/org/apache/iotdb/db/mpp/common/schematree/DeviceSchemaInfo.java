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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.List;
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
        .map(SchemaMeasurementNode::getSchema)
        .collect(Collectors.toList());
  }

  public List<MeasurementPath> getMeasurements() {
    return measurementNodeList.stream()
        .map(
            measurementNode -> {
              MeasurementPath measurementPath =
                  new MeasurementPath(
                      devicePath.concatNode(measurementNode.getName()),
                      measurementNode.getSchema());
              measurementPath.setUnderAlignedEntity(isAligned);
              if (measurementNode.getAlias() != null) {
                measurementPath.setMeasurementAlias(measurementNode.getAlias());
              }
              return measurementPath;
            })
        .collect(Collectors.toList());
  }
}
