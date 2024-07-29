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

package org.apache.iotdb.db.queryengine.plan.planner.plan.parameter;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.SelectIntoUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent.DEVICE_ALIGNMENT_INCONSISTENT_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent.DUPLICATE_TARGET_PATH_ERROR_MSG;

public class DeviceViewIntoPathDescriptor {

  // device -> List<(sourceColumn, targetPath)>
  private Map<String, List<Pair<String, PartialPath>>> deviceToSourceTargetPathPairListMap;

  // targetDevice -> isAlignedDevice
  private final Map<String, Boolean> targetDeviceToAlignedMap;

  // sourceColumn -> dataType (not serialize & deserialize)
  private Map<String, TSDataType> sourceToDataTypeMap;

  public DeviceViewIntoPathDescriptor() {
    this.deviceToSourceTargetPathPairListMap = new HashMap<>();
    this.targetDeviceToAlignedMap = new HashMap<>();
    this.sourceToDataTypeMap = new HashMap<>();
  }

  public DeviceViewIntoPathDescriptor(
      Map<String, List<Pair<String, PartialPath>>> deviceToSourceTargetPathPairListMap,
      Map<String, Boolean> targetDeviceToAlignedMap) {
    this.deviceToSourceTargetPathPairListMap = deviceToSourceTargetPathPairListMap;
    this.targetDeviceToAlignedMap = targetDeviceToAlignedMap;
  }

  public void specifyTargetDeviceMeasurement(
      PartialPath sourceDevice,
      PartialPath targetDevice,
      String sourceColumn,
      String targetMeasurement) {
    deviceToSourceTargetPathPairListMap
        .computeIfAbsent(sourceDevice.toString(), key -> new ArrayList<>())
        .add(new Pair<>(sourceColumn, targetDevice.concatAsMeasurementPath(targetMeasurement)));
  }

  public void specifyDeviceAlignment(String targetDevice, boolean isAligned) {
    if (targetDeviceToAlignedMap.containsKey(targetDevice)
        && targetDeviceToAlignedMap.get(targetDevice) != isAligned) {
      throw new SemanticException(DEVICE_ALIGNMENT_INCONSISTENT_ERROR_MSG);
    }
    targetDeviceToAlignedMap.put(targetDevice, isAligned);
  }

  public void recordSourceColumnDataType(String sourceColumn, TSDataType dataType) {
    sourceToDataTypeMap.put(sourceColumn, dataType);
  }

  public void validate() {
    List<PartialPath> targetPaths =
        deviceToSourceTargetPathPairListMap.values().stream()
            .flatMap(List::stream)
            .map(Pair::getRight)
            .collect(Collectors.toList());
    if (targetPaths.size() > new HashSet<>(targetPaths).size()) {
      throw new SemanticException(DUPLICATE_TARGET_PATH_ERROR_MSG);
    }
  }

  public void bindType(ISchemaTree targetSchemaTree) {
    Map<String, List<Pair<String, PartialPath>>> deviceToSourceTypeBoundTargetPathPairListMap =
        new HashMap<>();
    for (Map.Entry<String, List<Pair<String, PartialPath>>> sourceTargetEntry :
        this.deviceToSourceTargetPathPairListMap.entrySet()) {
      deviceToSourceTypeBoundTargetPathPairListMap.put(
          sourceTargetEntry.getKey(),
          SelectIntoUtils.bindTypeForSourceTargetPathPairList(
              sourceTargetEntry.getValue(), sourceToDataTypeMap, targetSchemaTree));
    }
    this.deviceToSourceTargetPathPairListMap = deviceToSourceTypeBoundTargetPathPairListMap;
  }

  public Map<String, List<Pair<String, PartialPath>>> getDeviceToSourceTargetPathPairListMap() {
    return deviceToSourceTargetPathPairListMap;
  }

  public Map<String, Map<PartialPath, Map<String, String>>> getSourceDeviceToTargetPathMap() {
    // sourceDevice -> targetPathToSourceMap (for each device)
    //  targetPathToSourceMap: targetDevice -> { targetMeasurement -> sourceColumn }
    Map<String, Map<PartialPath, Map<String, String>>> sourceDeviceToTargetPathMap =
        new HashMap<>();
    for (Map.Entry<String, List<Pair<String, PartialPath>>> sourceTargetEntry :
        deviceToSourceTargetPathPairListMap.entrySet()) {
      String sourceDevice = sourceTargetEntry.getKey();
      List<Pair<String, PartialPath>> sourceTargetPathPairList = sourceTargetEntry.getValue();

      Map<PartialPath, Map<String, String>> targetPathToSourceMap = new HashMap<>();
      for (Pair<String, PartialPath> sourceTargetPathPair : sourceTargetPathPairList) {
        String sourceColumn = sourceTargetPathPair.left;
        PartialPath targetDevice = sourceTargetPathPair.right.getDevicePath();
        String targetMeasurement = sourceTargetPathPair.right.getMeasurement();

        targetPathToSourceMap
            .computeIfAbsent(targetDevice, key -> new HashMap<>())
            .put(targetMeasurement, sourceColumn);
      }
      sourceDeviceToTargetPathMap.put(sourceDevice, targetPathToSourceMap);
    }
    return sourceDeviceToTargetPathMap;
  }

  public Map<String, Map<PartialPath, Map<String, TSDataType>>>
      getSourceDeviceToTargetPathDataTypeMap() {
    // sourceDevice -> targetPathToDataTypeMap (for each device)
    //  targetPathToSourceMap: targetDevice -> { targetMeasurement -> dataType }
    Map<String, Map<PartialPath, Map<String, TSDataType>>> sourceDeviceToTargetPathDataTypeMap =
        new HashMap<>();
    for (Map.Entry<String, List<Pair<String, PartialPath>>> sourceTargetEntry :
        deviceToSourceTargetPathPairListMap.entrySet()) {
      sourceDeviceToTargetPathDataTypeMap.put(
          sourceTargetEntry.getKey(),
          SelectIntoUtils.convertSourceTargetPathPairListToTargetPathDataTypeMap(
              sourceTargetEntry.getValue()));
    }
    return sourceDeviceToTargetPathDataTypeMap;
  }

  public Map<String, Boolean> getTargetDeviceToAlignedMap() {
    return targetDeviceToAlignedMap;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(deviceToSourceTargetPathPairListMap.size(), byteBuffer);
    for (Map.Entry<String, List<Pair<String, PartialPath>>> sourceTargetEntry :
        deviceToSourceTargetPathPairListMap.entrySet()) {
      ReadWriteIOUtils.write(sourceTargetEntry.getKey(), byteBuffer);

      List<Pair<String, PartialPath>> sourceTargetPathPairList = sourceTargetEntry.getValue();
      ReadWriteIOUtils.write(sourceTargetPathPairList.size(), byteBuffer);
      for (Pair<String, PartialPath> sourceTargetPathPair : sourceTargetPathPairList) {
        ReadWriteIOUtils.write(sourceTargetPathPair.left, byteBuffer);
        sourceTargetPathPair.right.serialize(byteBuffer);
      }
    }

    ReadWriteIOUtils.write(targetDeviceToAlignedMap.size(), byteBuffer);
    for (Map.Entry<String, Boolean> entry : targetDeviceToAlignedMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(deviceToSourceTargetPathPairListMap.size(), stream);
    for (Map.Entry<String, List<Pair<String, PartialPath>>> sourceTargetEntry :
        deviceToSourceTargetPathPairListMap.entrySet()) {
      ReadWriteIOUtils.write(sourceTargetEntry.getKey(), stream);

      List<Pair<String, PartialPath>> sourceTargetPathPairList = sourceTargetEntry.getValue();
      ReadWriteIOUtils.write(sourceTargetPathPairList.size(), stream);
      for (Pair<String, PartialPath> sourceTargetPathPair : sourceTargetPathPairList) {
        ReadWriteIOUtils.write(sourceTargetPathPair.left, stream);
        sourceTargetPathPair.right.serialize(stream);
      }
    }

    ReadWriteIOUtils.write(targetDeviceToAlignedMap.size(), stream);
    for (Map.Entry<String, Boolean> entry : targetDeviceToAlignedMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  public static DeviceViewIntoPathDescriptor deserialize(ByteBuffer byteBuffer) {
    int mapSize = ReadWriteIOUtils.readInt(byteBuffer);
    Map<String, List<Pair<String, PartialPath>>> deviceToSourceTargetPathPairListMap =
        new HashMap<>(mapSize);
    for (int i = 0; i < mapSize; i++) {
      String sourceDevice = ReadWriteIOUtils.readString(byteBuffer);
      int listSize = ReadWriteIOUtils.readInt(byteBuffer);
      List<Pair<String, PartialPath>> sourceTargetPathPairList = new ArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        String sourceColumn = ReadWriteIOUtils.readString(byteBuffer);
        PartialPath targetPath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
        sourceTargetPathPairList.add(new Pair<>(sourceColumn, targetPath));
      }
      deviceToSourceTargetPathPairListMap.put(sourceDevice, sourceTargetPathPairList);
    }

    mapSize = ReadWriteIOUtils.readInt(byteBuffer);
    Map<String, Boolean> targetDeviceToAlignedMap = new HashMap<>(mapSize);
    for (int i = 0; i < mapSize; i++) {
      targetDeviceToAlignedMap.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readBool(byteBuffer));
    }
    return new DeviceViewIntoPathDescriptor(
        deviceToSourceTargetPathPairListMap, targetDeviceToAlignedMap);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeviceViewIntoPathDescriptor that = (DeviceViewIntoPathDescriptor) o;
    return deviceToSourceTargetPathPairListMap.equals(that.deviceToSourceTargetPathPairListMap)
        && targetDeviceToAlignedMap.equals(that.targetDeviceToAlignedMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceToSourceTargetPathPairListMap, targetDeviceToAlignedMap);
  }
}
