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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.statistics.StatisticsManager;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.UNKNOWN_DATATYPE;

public class OperatorGeneratorUtil {

  public static long calculateStatementSizePerLine(List<TSDataType> dataTypeList) {
    long maxStatementSize = Long.BYTES;
    for (TSDataType dataType : dataTypeList) {
      maxStatementSize += getValueSizePerLine(dataType);
    }
    return maxStatementSize;
  }

  public static long calculateStatementSizePerLine(
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap) {
    long maxStatementSize = Long.BYTES;
    List<TSDataType> dataTypes =
        targetPathToDataTypeMap.values().stream()
            .flatMap(stringTSDataTypeMap -> stringTSDataTypeMap.values().stream())
            .collect(Collectors.toList());
    for (TSDataType dataType : dataTypes) {
      maxStatementSize += getValueSizePerLine(dataType);
    }
    return maxStatementSize;
  }

  private static long getValueSizePerLine(TSDataType tsDataType) {
    switch (tsDataType) {
      case INT32:
      case DATE:
        return Integer.BYTES;
      case INT64:
      case TIMESTAMP:
        return Long.BYTES;
      case FLOAT:
        return Float.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case BOOLEAN:
        return Byte.BYTES;
      case TEXT:
      case BLOB:
      case STRING:
        return StatisticsManager.getInstance().getMaxBinarySizeInBytes();
      default:
        throw new UnsupportedOperationException(UNKNOWN_DATATYPE + tsDataType);
    }
  }
}
