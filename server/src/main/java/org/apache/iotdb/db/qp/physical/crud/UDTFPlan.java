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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.binary.BinaryExpression;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.NegationExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UDTFPlan extends RawDataQueryPlan implements UDFPlan {

  protected final ZoneId zoneId;

  protected Map<String, UDTFExecutor> columnName2Executor = new HashMap<>();
  protected Map<Integer, UDTFExecutor> originalOutputColumnIndex2Executor = new HashMap<>();

  protected Map<Integer, Integer> datasetOutputIndexToResultColumnIndex = new HashMap<>();

  protected Map<String, Integer> pathNameToReaderIndex;

  public UDTFPlan(ZoneId zoneId) {
    super();
    this.zoneId = zoneId;
    setOperatorType(Operator.OperatorType.UDTF);
  }

  @Override
  public void deduplicate(PhysicalGenerator physicalGenerator) throws MetadataException {
    // sort paths by device, to accelerate the metadata read process
    List<Pair<PartialPath, Integer>> indexedPaths = new ArrayList<>();
    for (int i = 0; i < resultColumns.size(); i++) {
      for (PartialPath path : resultColumns.get(i).collectPaths()) {
        indexedPaths.add(new Pair<>(path, i));
      }
    }
    indexedPaths.sort(Comparator.comparing(pair -> pair.left));

    Map<String, Integer> pathNameToReaderIndex = new HashMap<>();
    Set<String> columnForReaderSet = new HashSet<>();
    Set<String> columnForDisplaySet = new HashSet<>();

    for (Pair<PartialPath, Integer> indexedPath : indexedPaths) {
      PartialPath originalPath = indexedPath.left;
      Integer originalIndex = indexedPath.right;

      boolean isUdf =
          !(resultColumns.get(originalIndex).getExpression() instanceof TimeSeriesOperand);

      String columnForReader = getColumnForReaderFromPath(originalPath, originalIndex);
      if (!columnForReaderSet.contains(columnForReader)) {
        addDeduplicatedPaths(originalPath);
        addDeduplicatedDataTypes(
            isUdf ? IoTDB.metaManager.getSeriesType(originalPath) : dataTypes.get(originalIndex));
        pathNameToReaderIndex.put(columnForReader, pathNameToReaderIndex.size());
        columnForReaderSet.add(columnForReader);
      }

      String columnForDisplay = getColumnForDisplay(columnForReader, originalIndex);
      if (!columnForDisplaySet.contains(columnForDisplay)) {
        int datasetOutputIndex = getPathToIndex().size();
        setColumnNameToDatasetOutputIndex(columnForDisplay, datasetOutputIndex);
        setDatasetOutputIndexToResultColumnIndex(datasetOutputIndex, originalIndex);
        columnForDisplaySet.add(columnForDisplay);
      }
    }

    setPathNameToReaderIndex(pathNameToReaderIndex);
  }

  private void setDatasetOutputIndexToResultColumnIndex(
      int datasetOutputIndex, Integer originalIndex) {
    datasetOutputIndexToResultColumnIndex.put(datasetOutputIndex, originalIndex);
  }

  @Override
  public void constructUdfExecutors(List<ResultColumn> resultColumns) {
    for (int i = 0; i < resultColumns.size(); ++i) {
      Expression expression = resultColumns.get(i).getExpression();
      if (!(expression instanceof FunctionExpression)) {
        continue;
      }

      String columnName = expression.toString();
      columnName2Executor.computeIfAbsent(
          columnName, k -> new UDTFExecutor((FunctionExpression) expression, zoneId));
      originalOutputColumnIndex2Executor.put(i, columnName2Executor.get(columnName));
    }
  }

  @Override
  public void initializeUdfExecutors(long queryId, float collectorMemoryBudgetInMB)
      throws QueryProcessException {
    Collection<UDTFExecutor> executors = columnName2Executor.values();
    collectorMemoryBudgetInMB /= executors.size();

    UDFRegistrationService.getInstance().acquireRegistrationLock();
    // This statement must be surrounded by the registration lock.
    UDFClassLoaderManager.getInstance().initializeUDFQuery(queryId);
    try {
      for (UDTFExecutor executor : executors) {
        executor.beforeStart(queryId, collectorMemoryBudgetInMB);
      }
    } finally {
      UDFRegistrationService.getInstance().releaseRegistrationLock();
    }
  }

  @Override
  public void finalizeUDFExecutors(long queryId) {
    try {
      for (UDTFExecutor executor : columnName2Executor.values()) {
        executor.beforeDestroy();
      }
    } finally {
      UDFClassLoaderManager.getInstance().finalizeUDFQuery(queryId);
    }
  }

  public TSDataType getOriginalOutputColumnDataType(int originalOutputColumn) {
    Expression expression = resultColumns.get(originalOutputColumn).getExpression();
    // UDF query
    if (expression instanceof FunctionExpression) {
      return getExecutorByOriginalOutputColumnIndex(originalOutputColumn)
          .getConfigurations()
          .getOutputDataType();
    }
    // arithmetic binary query
    if (expression instanceof BinaryExpression) {
      return TSDataType.DOUBLE;
    }
    // arithmetic negation query
    if (expression instanceof NegationExpression) {
      return getDeduplicatedDataTypes()
          .get(getReaderIndex(((NegationExpression) expression).getExpression().toString()));
    }
    // raw query
    return getDeduplicatedDataTypes().get(getReaderIndex(expression.toString()));
  }

  public UDTFExecutor getExecutorByOriginalOutputColumnIndex(int originalOutputColumn) {
    return originalOutputColumnIndex2Executor.get(originalOutputColumn);
  }

  public ResultColumn getResultColumnByDatasetOutputIndex(int datasetOutputIndex) {
    return resultColumns.get(datasetOutputIndexToResultColumnIndex.get(datasetOutputIndex));
  }

  public UDTFExecutor getExecutorByDataSetOutputColumnIndex(int datasetOutputIndex) {
    return columnName2Executor.get(
        getResultColumnByDatasetOutputIndex(datasetOutputIndex).getResultColumnName());
  }

  public String getRawQueryColumnNameByDatasetOutputColumnIndex(int datasetOutputIndex) {
    return getResultColumnByDatasetOutputIndex(datasetOutputIndex).getResultColumnName();
  }

  public boolean isUdfColumn(int datasetOutputIndex) {
    return getResultColumnByDatasetOutputIndex(datasetOutputIndex).getExpression()
        instanceof FunctionExpression;
  }

  public boolean isArithmeticColumn(int datasetOutputIndex) {
    Expression expression = getResultColumnByDatasetOutputIndex(datasetOutputIndex).getExpression();
    return expression instanceof BinaryExpression || expression instanceof NegationExpression;
  }

  public int getReaderIndex(String pathName) {
    return pathNameToReaderIndex.get(pathName);
  }

  public void setPathNameToReaderIndex(Map<String, Integer> pathNameToReaderIndex) {
    this.pathNameToReaderIndex = pathNameToReaderIndex;
  }

  @Override
  public boolean isRawQuery() {
    return false;
  }
}
