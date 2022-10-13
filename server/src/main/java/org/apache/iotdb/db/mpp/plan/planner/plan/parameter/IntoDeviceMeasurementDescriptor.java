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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.mpp.plan.statement.component.IntoComponent.DUPLICATE_TARGET_DEVICE_ERROR_MSG;
import static org.apache.iotdb.db.mpp.plan.statement.component.IntoComponent.DUPLICATE_TARGET_PATH_ERROR_MSG;

public class IntoDeviceMeasurementDescriptor extends IntoPathDescriptor {

  private final Map<PartialPath, PartialPath> targetDeviceToSourceDeviceMap;

  public IntoDeviceMeasurementDescriptor() {
    super();
    this.targetDeviceToSourceDeviceMap = new HashMap<>();
  }

  public void specifyTargetDevice(PartialPath sourceDevice, PartialPath targetDevice) {
    if (targetDeviceToSourceDeviceMap.containsKey(targetDevice)
        && !targetDeviceToSourceDeviceMap.get(targetDevice).equals(sourceDevice)) {
      throw new SemanticException(DUPLICATE_TARGET_DEVICE_ERROR_MSG);
    }
    targetDeviceToSourceDeviceMap.put(targetDevice, sourceDevice);
  }

  public void specifyTargetMeasurement(
      PartialPath targetDevice, String sourceColumn, String targetMeasurement) {
    Map<String, String> measurementToSourceColumnMap =
        targetPathToSourceMap.computeIfAbsent(targetDevice, key -> new HashMap<>());
    if (measurementToSourceColumnMap.containsKey(targetMeasurement)) {
      throw new SemanticException(DUPLICATE_TARGET_PATH_ERROR_MSG);
    }
    measurementToSourceColumnMap.put(targetMeasurement, sourceColumn);
  }
}
