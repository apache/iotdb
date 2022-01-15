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
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UDTFPlan extends RawDataQueryPlan implements UDFPlan {

  protected final ZoneId zoneId;

  protected Map<String, UDTFExecutor> expressionName2Executor = new HashMap<>();
  protected Map<Integer, Integer> datasetOutputIndexToResultColumnIndex = new HashMap<>();
  protected Map<String, Integer> pathNameToReaderIndex = new HashMap<>();

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

    Set<String> columnForReaderSet = new HashSet<>();
    Set<String> columnForDisplaySet = new HashSet<>();

    for (Pair<PartialPath, Integer> indexedPath : indexedPaths) {
      PartialPath originalPath = indexedPath.left;
      Integer originalIndex = indexedPath.right;

      String columnForReader = originalPath.getFullPath();
      if (!columnForReaderSet.contains(columnForReader)) {
        addDeduplicatedPaths(originalPath);
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

    // Aligned timeseries is not supported in current query for now.
    // To judge whether an aligned timeseries is used, we need to traversal all the paths in
    // deduplicatedPaths.
    for (PartialPath path : getDeduplicatedPaths()) {
      MeasurementPath measurementPath = (MeasurementPath) path;
      if (measurementPath.isUnderAlignedEntity()) {
        throw new MetadataException(
            "Aligned timeseries is not supported in current query for now.");
      }
    }
  }

  @Override
  public List<TSDataType> getWideQueryHeaders(
      List<String> respColumns, List<String> respSgColumns, boolean isJdbcQuery, BitSet aliasList) {
    List<TSDataType> seriesTypes = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      respColumns.add(resultColumns.get(i).getResultColumnName());
      seriesTypes.add(resultColumns.get(i).getDataType());
    }
    return seriesTypes;
  }

  protected void setDatasetOutputIndexToResultColumnIndex(
      int datasetOutputIndex, Integer originalIndex) {
    datasetOutputIndexToResultColumnIndex.put(datasetOutputIndex, originalIndex);
  }

  @Override
  public List<PartialPath> getAuthPaths() {
    Set<PartialPath> authPaths = new HashSet<>();
    for (ResultColumn resultColumn : resultColumns) {
      authPaths.addAll(resultColumn.collectPaths());
    }
    return new ArrayList<>(authPaths);
  }

  @Override
  public void constructUdfExecutors(List<ResultColumn> resultColumns) {
    for (ResultColumn resultColumn : resultColumns) {
      resultColumn.getExpression().constructUdfExecutors(expressionName2Executor, zoneId);
    }
  }

  @Override
  public void finalizeUDFExecutors(long queryId) {
    try {
      for (UDTFExecutor executor : expressionName2Executor.values()) {
        executor.beforeDestroy();
      }
    } finally {
      UDFClassLoaderManager.getInstance().finalizeUDFQuery(queryId);
    }
  }

  public ResultColumn getResultColumnByDatasetOutputIndex(int datasetOutputIndex) {
    return resultColumns.get(datasetOutputIndexToResultColumnIndex.get(datasetOutputIndex));
  }

  public UDTFExecutor getExecutorByFunctionExpression(FunctionExpression functionExpression) {
    return expressionName2Executor.get(functionExpression.getExpressionString());
  }

  public int getReaderIndex(String pathName) {
    return pathNameToReaderIndex.get(pathName);
  }

  public int getReaderIndexByExpressionName(String expressionName) {
    return pathNameToReaderIndex.get(expressionName);
  }
}
