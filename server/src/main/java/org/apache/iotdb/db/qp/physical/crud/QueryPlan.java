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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.SpecialClauseComponent;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.google.common.primitives.Bytes;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class QueryPlan extends PhysicalPlan {

  public static final String WITHOUT_NULL_FILTER_ERROR_MESSAGE =
      "The without null columns don't match the columns queried.If has alias, please use the alias.";

  protected List<ResultColumn> resultColumns = null;
  protected List<MeasurementPath> paths = null;

  private boolean alignByTime = true; // for disable align sql

  private int rowLimit = 0;
  private int rowOffset = 0;

  private boolean ascending = true;

  private Map<String, Integer> pathToIndex = new HashMap<>();

  protected Set<Integer>
      withoutNullColumnsIndex; // index set that withoutNullColumns for output data columns

  private boolean enableRedirect = false;
  private boolean enableTracing = false;

  // if true, we don't need the row whose any column is null
  private boolean withoutAnyNull;

  // if true, we don't need the row whose all columns are null
  private boolean withoutAllNull;

  public QueryPlan() {
    super(Operator.OperatorType.QUERY);
    setQuery(true);
  }

  public Set<Integer> getWithoutNullColumnsIndex() {
    return withoutNullColumnsIndex;
  }

  public abstract void deduplicate(PhysicalGenerator physicalGenerator) throws MetadataException;

  public abstract void convertSpecialClauseValues(SpecialClauseComponent specialClauseComponent)
      throws QueryProcessException;

  /** Construct the header of result set. Return TSExecuteStatementResp. */
  public TSExecuteStatementResp getTSExecuteStatementResp(boolean isJdbcQuery)
      throws TException, MetadataException {
    List<String> respColumns = new ArrayList<>();
    List<String> columnsTypes = new ArrayList<>();

    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);

    List<String> respSgColumns = new ArrayList<>();
    BitSet aliasMap = new BitSet();
    List<TSDataType> seriesTypes =
        getWideQueryHeaders(respColumns, respSgColumns, isJdbcQuery, aliasMap);
    for (TSDataType seriesType : seriesTypes) {
      columnsTypes.add(seriesType.toString());
    }
    resp.setColumnNameIndexMap(getPathToIndex());
    resp.setSgColumns(respSgColumns);
    List<Byte> byteList = new ArrayList<>();
    byteList.addAll(Bytes.asList(aliasMap.toByteArray()));
    resp.setAliasColumns(byteList);

    resp.setColumns(respColumns);
    resp.setDataTypeList(columnsTypes);
    return resp;
  }

  public List<TSDataType> getWideQueryHeaders(
      List<String> respColumns, List<String> respSgColumns, boolean isJdbcQuery, BitSet aliasList)
      throws TException, MetadataException {
    List<TSDataType> seriesTypes = new ArrayList<>();
    for (int i = 0; i < resultColumns.size(); ++i) {
      if (isJdbcQuery) {
        // Separate sgName from the name of resultColumn to reduce the network IO
        String sgName = IoTDB.metaManager.getBelongedStorageGroup(getPaths().get(i)).getFullPath();
        respSgColumns.add(sgName);
        if (resultColumns.get(i).getAlias() == null) {
          respColumns.add(
              resultColumns.get(i).getResultColumnName().substring(sgName.length() + 1));
        } else {
          aliasList.set(i);
          respColumns.add(resultColumns.get(i).getResultColumnName());
        }
      } else {
        respColumns.add(resultColumns.get(i).getResultColumnName());
      }
      seriesTypes.add(paths.get(i).getSeriesType());
    }
    return seriesTypes;
  }

  @Override
  public List<MeasurementPath> getPaths() {
    return paths;
  }

  @Override
  public void setPaths(List<PartialPath> paths) {
    List<MeasurementPath> measurementPaths = new ArrayList<>();
    for (PartialPath path : paths) {
      measurementPaths.add((MeasurementPath) path);
    }
    this.paths = measurementPaths;
  }

  public List<TSDataType> getDataTypes() {
    return SchemaUtils.getSeriesTypesByPaths(paths);
  }

  public int getRowLimit() {
    return rowLimit;
  }

  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  public int getRowOffset() {
    return rowOffset;
  }

  public void setRowOffset(int rowOffset) {
    this.rowOffset = rowOffset;
  }

  public boolean hasLimit() {
    return rowLimit > 0;
  }

  public boolean isAlignByTime() {
    return alignByTime;
  }

  public void setAlignByTime(boolean align) {
    alignByTime = align;
  }

  public void setColumnNameToDatasetOutputIndex(String columnName, Integer index) {
    pathToIndex.put(columnName, index);
  }

  public boolean isGroupByLevel() {
    return false;
  }

  public Map<String, Integer> getPathToIndex() {
    return pathToIndex;
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }

  public String getColumnForReaderFromPath(PartialPath path, int pathIndex) {
    ResultColumn resultColumn = resultColumns.get(pathIndex);
    return resultColumn.hasAlias() ? resultColumn.getAlias() : path.getFullPath();
  }

  public String getColumnForDisplay(String columnForReader, int pathIndex) {
    return resultColumns.get(pathIndex).getResultColumnName();
  }

  public boolean isEnableRedirect() {
    return enableRedirect;
  }

  public void setEnableRedirect(boolean enableRedirect) {
    this.enableRedirect = enableRedirect;
  }

  public boolean isEnableTracing() {
    return enableTracing;
  }

  public void setEnableTracing(boolean enableTracing) {
    this.enableTracing = enableTracing;
  }

  public List<ResultColumn> getResultColumns() {
    return resultColumns;
  }

  public void setResultColumns(List<ResultColumn> resultColumns) {
    this.resultColumns = resultColumns;
  }

  public boolean isWithoutAnyNull() {
    return withoutAnyNull;
  }

  public void setWithoutAnyNull(boolean withoutAnyNull) {
    this.withoutAnyNull = withoutAnyNull;
  }

  public boolean isWithoutAllNull() {
    return withoutAllNull;
  }

  public void setWithoutAllNull(boolean withoutAllNull) {
    this.withoutAllNull = withoutAllNull;
  }
}
