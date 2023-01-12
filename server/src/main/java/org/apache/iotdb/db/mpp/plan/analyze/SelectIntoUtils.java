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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.commons.conf.IoTDBConstant.DOUBLE_COLONS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.LEVELED_PATH_TEMPLATE_PATTERN;

public class SelectIntoUtils {

  public static PartialPath constructTargetPath(
      PartialPath sourcePath, PartialPath deviceTemplate, String measurementTemplate) {
    PartialPath targetDevice = constructTargetDevice(sourcePath.getDevicePath(), deviceTemplate);
    String targetMeasurement = constructTargetMeasurement(sourcePath, measurementTemplate);
    return targetDevice.concatNode(targetMeasurement);
  }

  public static PartialPath constructTargetDevice(
      PartialPath sourceDevice, PartialPath deviceTemplate) {
    String[] sourceNodes = sourceDevice.getNodes();
    String[] templateNodes = deviceTemplate.getNodes();

    List<String> targetNodes = new ArrayList<>();
    for (int nodeIndex = 0; nodeIndex < templateNodes.length; nodeIndex++) {
      String curNode = templateNodes[nodeIndex];
      if (curNode.equals(DOUBLE_COLONS)) {
        if (nodeIndex != templateNodes.length - 1) {
          throw new SemanticException(
              "select into: placeholder `::` can only be used at the end of the path.");
        }
        for (; nodeIndex < sourceNodes.length; nodeIndex++) {
          targetNodes.add(sourceNodes[nodeIndex]);
        }
        break;
      }

      String resNode = applyLevelPlaceholder(curNode, sourceNodes);
      targetNodes.add(resNode);
    }
    return new PartialPath(targetNodes.toArray(new String[0]));
  }

  public static String constructTargetMeasurement(
      PartialPath sourcePath, String measurementTemplate) {
    if (measurementTemplate.equals(DOUBLE_COLONS)) {
      return sourcePath.getMeasurement();
    }
    return applyLevelPlaceholder(measurementTemplate, sourcePath.getNodes());
  }

  private static String applyLevelPlaceholder(String templateNode, String[] sourceNodes) {
    String resNode = templateNode;
    Matcher matcher = LEVELED_PATH_TEMPLATE_PATTERN.matcher(resNode);
    while (matcher.find()) {
      String param = matcher.group();
      int index;
      try {
        index = Integer.parseInt(param.substring(2, param.length() - 1).trim());
      } catch (NumberFormatException e) {
        throw new SemanticException("select into: the i of ${i} should be an integer.");
      }
      if (index < 1 || index >= sourceNodes.length) {
        throw new SemanticException(
            "select into: the i of ${i} should be greater than 0 and equal to or less than the length of queried path prefix.");
      }
      resNode = matcher.replaceFirst(sourceNodes[index]);
      matcher = LEVELED_PATH_TEMPLATE_PATTERN.matcher(resNode);
    }
    return resNode;
  }

  public static boolean checkIsAllRawSeriesQuery(List<Expression> expressions) {
    for (Expression expression : expressions) {
      if (!(expression instanceof TimeSeriesOperand)) {
        return false;
      }
    }
    return true;
  }

  public static List<Pair<String, PartialPath>> bindTypeForSourceTargetPathPairList(
      List<Pair<String, PartialPath>> sourceTargetPathPairList,
      Map<String, TSDataType> sourceToDataTypeMap,
      ISchemaTree targetSchemaTree,
      Map<String, Boolean> targetDeviceToAlignedMap) {
    List<Pair<String, PartialPath>> sourceTypeBoundTargetPathPairList = new ArrayList<>();
    for (Pair<String, PartialPath> sourceTargetPathPair : sourceTargetPathPairList) {
      String sourceColumn = sourceTargetPathPair.left;
      TSDataType sourceColumnType = sourceToDataTypeMap.get(sourceColumn);

      MeasurementPath targetPathWithSchema;
      PartialPath targetPath = sourceTargetPathPair.right;
      List<MeasurementPath> actualTargetPaths =
          targetSchemaTree.searchMeasurementPaths(targetPath).left;
      if (actualTargetPaths.isEmpty()) {
        targetPathWithSchema = new MeasurementPath(targetPath, sourceColumnType);
      } else {
        checkState(actualTargetPaths.size() == 1);
        MeasurementPath actualTargetPath = actualTargetPaths.get(0);
        if (!TypeInferenceUtils.canAutoCast(sourceColumnType, actualTargetPath.getSeriesType())) {
          throw new SemanticException(
              String.format(
                  "The data type of target path (%s[%s]) is not compatible with the data type of source column (%s[%s]).",
                  targetPath, actualTargetPath.getSeriesType(), sourceColumn, sourceColumnType));
        }
        boolean actualTargetPathAlignment = actualTargetPath.isUnderAlignedEntity();
        String targetDevice = targetPath.getDevice();
        if (targetDeviceToAlignedMap.get(targetDevice) != actualTargetPathAlignment) {
          throw new SemanticException(
              String.format(
                  "The specified alignment property of the target device (%s) conflicts with the actual (isAligned = %s).",
                  targetDevice, actualTargetPathAlignment));
        }
        targetPathWithSchema = actualTargetPath;
      }
      sourceTypeBoundTargetPathPairList.add(new Pair<>(sourceColumn, targetPathWithSchema));
    }
    return sourceTypeBoundTargetPathPairList;
  }

  public static Map<PartialPath, Map<String, TSDataType>>
      convertSourceTargetPathPairListToTargetPathDataTypeMap(
          List<Pair<String, PartialPath>> sourceTargetPathPairList) {
    // targetDevice -> { targetMeasurement -> dataType }
    Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap = new HashMap<>();

    for (Pair<String, PartialPath> sourceTargetPathPair : sourceTargetPathPairList) {
      PartialPath targetPath = sourceTargetPathPair.right;
      targetPathToDataTypeMap
          .computeIfAbsent(targetPath.getDevicePath(), key -> new HashMap<>())
          .put(targetPath.getMeasurement(), targetPath.getSeriesType());
    }
    return targetPathToDataTypeMap;
  }
}
