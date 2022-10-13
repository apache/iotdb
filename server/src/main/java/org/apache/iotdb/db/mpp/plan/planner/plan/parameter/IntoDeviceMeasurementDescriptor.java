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
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.mpp.plan.statement.component.IntoComponent.DUPLICATE_TARGET_PATH_ERROR_MSG;

public class IntoDeviceMeasurementDescriptor {

  private final Map<PartialPath, Map<String, Pair<PartialPath, String>>> targetPathToSourceMap;
  protected final Map<PartialPath, Boolean> deviceToAlignedMap;

  public IntoDeviceMeasurementDescriptor() {
    this.targetPathToSourceMap = new HashMap<>();
    this.deviceToAlignedMap = new HashMap<>();
  }

  public void specifyTargetDeviceMeasurement(
      PartialPath sourceDevice,
      PartialPath targetDevice,
      String sourceColumn,
      String targetMeasurement) {
    Map<String, Pair<PartialPath, String>> measurementToSourceColumnMap =
        targetPathToSourceMap.computeIfAbsent(targetDevice, key -> new HashMap<>());
    if (measurementToSourceColumnMap.containsKey(targetMeasurement)) {
      throw new SemanticException(DUPLICATE_TARGET_PATH_ERROR_MSG);
    }
    measurementToSourceColumnMap.put(targetMeasurement, new Pair<>(sourceDevice, sourceColumn));
  }

  public void specifyDeviceAlignment(PartialPath targetDevice, boolean isAligned) {
    if (deviceToAlignedMap.containsKey(targetDevice)
        && deviceToAlignedMap.get(targetDevice) != isAligned) {
      throw new SemanticException(
          "select into: alignment property must be the same for the same device.");
    }
    deviceToAlignedMap.put(targetDevice, isAligned);
  }
}
