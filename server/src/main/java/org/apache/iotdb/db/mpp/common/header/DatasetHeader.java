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

package org.apache.iotdb.db.mpp.common.header;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.google.common.primitives.Bytes;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** The header of query result dataset. */
public class DatasetHeader {

  // column names, data types and aliases of result dataset
  private final List<ColumnHeader> columnHeaders;

  // indicate whether the result dataset contain timestamp column
  private final boolean isIgnoreTimestamp;

  // map from output column to output tsBlock index
  private Map<String, Integer> columnToTsBlockIndexMap;

  public DatasetHeader(List<ColumnHeader> columnHeaders, boolean isIgnoreTimestamp) {
    this.columnHeaders = columnHeaders;
    this.isIgnoreTimestamp = isIgnoreTimestamp;
  }

  public List<ColumnHeader> getColumnHeaders() {
    return columnHeaders;
  }

  public boolean isIgnoreTimestamp() {
    return isIgnoreTimestamp;
  }

  public void setColumnToTsBlockIndexMap(List<String> outputColumnNames) {
    this.columnToTsBlockIndexMap = new HashMap<>();
    for (int i = 0; i < outputColumnNames.size(); i++) {
      columnToTsBlockIndexMap.put(outputColumnNames.get(i), i);
    }
  }

  public List<String> getRespColumns() {
    return columnHeaders.stream()
        .map(ColumnHeader::getColumnNameWithAlias)
        .collect(Collectors.toList());
  }

  public List<String> getColumnNameWithoutAlias() {
    return columnHeaders.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList());
  }

  public List<String> getRespDataTypeList() {
    return columnHeaders.stream()
        .map(ColumnHeader::getColumnType)
        .map(Objects::toString)
        .collect(Collectors.toList());
  }

  public List<TSDataType> getRespDataTypes() {
    return columnHeaders.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList());
  }

  public List<Byte> getRespAliasColumns() {
    BitSet aliasMap = new BitSet();
    for (int i = 0; i < columnHeaders.size(); ++i) {
      if (columnHeaders.get(i).hasAlias()) {
        aliasMap.set(i);
      }
    }
    return new ArrayList<>(Bytes.asList(aliasMap.toByteArray()));
  }

  public Map<String, Integer> getColumnNameIndexMap() {
    if (columnToTsBlockIndexMap == null || columnToTsBlockIndexMap.isEmpty()) {
      return columnToTsBlockIndexMap;
    }

    Map<String, Integer> columnNameIndexMap = new HashMap<>();
    for (ColumnHeader columnHeader : columnHeaders) {
      columnNameIndexMap.put(
          columnHeader.getColumnNameWithAlias(),
          columnToTsBlockIndexMap.get(columnHeader.getColumnName()));
    }
    return columnNameIndexMap;
  }

  public int getOutputValueColumnCount() {
    return (int) columnHeaders.stream().map(ColumnHeader::getColumnName).distinct().count();
  }
}
