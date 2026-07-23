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
import org.apache.iotdb.db.utils.TypeServices;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    return TypeServices.STATEMENT_VALUE_SIZE_PER_LINE_SERVICE
        .call(Type.fromTsDataType(tsDataType))
        .getAsLong();
  }
}
