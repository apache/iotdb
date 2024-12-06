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

package org.apache.iotdb.db.queryengine.common.header;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;

import com.google.common.primitives.Bytes;
import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The header of query result dataset. */
public class DatasetHeader {

  public static final DatasetHeader EMPTY_HEADER = new DatasetHeader(new ArrayList<>(), false);

  // column names, data types and aliases of result dataset
  // !!attention!! there may exist duplicated column names in table model
  private final List<ColumnHeader> columnHeaders;

  // indicate whether the result dataset contain timestamp column
  private final boolean isIgnoreTimestamp;

  // map from output column to output tsBlock index
  private Map<String, Integer> columnToTsBlockIndexMap;
  private List<Integer> columnIndex2TsBlockColumnIndexList;

  // cached field for create response
  private List<String> respColumns;
  private List<TSDataType> respDataTypes;
  private List<String> respDataTypeList;
  private List<Byte> respAliasColumns;
  private Map<String, Integer> columnNameIndexMap;
  private Integer outputValueColumnCount;

  public DatasetHeader(List<ColumnHeader> columnHeaders, boolean isIgnoreTimestamp) {
    this.columnHeaders = columnHeaders;
    this.isIgnoreTimestamp = isIgnoreTimestamp;
    int size = columnHeaders.size();
    this.columnIndex2TsBlockColumnIndexList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      columnIndex2TsBlockColumnIndexList.add(i);
    }
  }

  public List<ColumnHeader> getColumnHeaders() {
    return columnHeaders;
  }

  public boolean isIgnoreTimestamp() {
    return isIgnoreTimestamp;
  }

  public void setTreeColumnToTsBlockIndexMap(List<String> outputColumnNames) {
    this.columnToTsBlockIndexMap = new HashMap<>(outputColumnNames.size());
    for (int i = 0; i < outputColumnNames.size(); i++) {
      columnToTsBlockIndexMap.put(outputColumnNames.get(i), i);
    }

    this.columnIndex2TsBlockColumnIndexList = new ArrayList<>(columnHeaders.size());
    for (ColumnHeader columnHeader : columnHeaders) {
      columnIndex2TsBlockColumnIndexList.add(
          columnToTsBlockIndexMap.get(columnHeader.getColumnName()));
    }
  }

  public void setTableColumnToTsBlockIndexMap(OutputNode outputNode) {
    List<Symbol> childOutputSymbols = outputNode.getChild().getOutputSymbols();
    Map<Symbol, Integer> outputSymbolsIndexMap = new HashMap<>(childOutputSymbols.size());
    for (int i = 0; i < childOutputSymbols.size(); i++) {
      outputSymbolsIndexMap.put(childOutputSymbols.get(i), i);
    }

    this.columnToTsBlockIndexMap = new HashMap<>();
    this.columnIndex2TsBlockColumnIndexList =
        new ArrayList<>(outputNode.getOutputColumnNames().size());
    for (int i = 0; i < outputNode.getOutputColumnNames().size(); i++) {
      int index = outputSymbolsIndexMap.get(outputNode.getOutputSymbols().get(i));
      columnToTsBlockIndexMap.put(outputNode.getOutputColumnNames().get(i), index);
      columnIndex2TsBlockColumnIndexList.add(index);
    }
  }

  public List<String> getRespColumns() {
    if (respColumns == null) {
      respColumns = new ArrayList<>();
      for (ColumnHeader columnHeader : columnHeaders) {
        respColumns.add(columnHeader.getColumnNameWithAlias());
      }
    }
    return respColumns;
  }

  public List<TSDataType> getRespDataTypes() {
    if (respDataTypes == null) {
      respDataTypes = new ArrayList<>();
      for (ColumnHeader columnHeader : columnHeaders) {
        respDataTypes.add(columnHeader.getColumnType());
      }
    }
    return respDataTypes;
  }

  public List<String> getRespDataTypeList() {
    if (respDataTypeList == null) {
      respDataTypeList = new ArrayList<>();
      for (ColumnHeader columnHeader : columnHeaders) {
        respDataTypeList.add(columnHeader.getColumnType().toString());
      }
    }
    return respDataTypeList;
  }

  public List<Byte> getRespAliasColumns() {
    if (respAliasColumns == null) {
      BitSet aliasMap = new BitSet();
      for (int i = 0; i < columnHeaders.size(); ++i) {
        if (columnHeaders.get(i).hasAlias()) {
          aliasMap.set(i);
        }
      }
      respAliasColumns = new ArrayList<>(Bytes.asList(aliasMap.toByteArray()));
    }
    return respAliasColumns;
  }

  public Map<String, Integer> getColumnNameIndexMap() {
    if (columnToTsBlockIndexMap == null || columnToTsBlockIndexMap.isEmpty()) {
      return columnToTsBlockIndexMap;
    }

    if (columnNameIndexMap == null) {
      columnNameIndexMap = new HashMap<>();
      for (ColumnHeader columnHeader : columnHeaders) {
        columnNameIndexMap.put(
            columnHeader.getColumnNameWithAlias(),
            columnToTsBlockIndexMap.get(columnHeader.getColumnName()));
      }
    }
    return columnNameIndexMap;
  }

  public int getOutputValueColumnCount() {
    if (outputValueColumnCount == null) {
      HashSet<String> columnNameSet = new HashSet<>();
      for (ColumnHeader columnHeader : columnHeaders) {
        columnNameSet.add(columnHeader.getColumnName());
      }
      outputValueColumnCount = columnNameSet.size();
    }
    return outputValueColumnCount;
  }

  public List<Integer> getColumnIndex2TsBlockColumnIndexList() {
    return columnIndex2TsBlockColumnIndexList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetHeader that = (DatasetHeader) o;
    return isIgnoreTimestamp == that.isIgnoreTimestamp && columnHeaders.equals(that.columnHeaders);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnHeaders, isIgnoreTimestamp);
  }
}
