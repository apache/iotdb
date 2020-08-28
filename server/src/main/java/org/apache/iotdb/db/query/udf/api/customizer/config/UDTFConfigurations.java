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

package org.apache.iotdb.db.query.udf.api.customizer.config;

import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.DataPointBatchIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.DataPointIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowRecordBatchIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowRecordIterationStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public class UDTFConfigurations extends UDFConfigurations {

  protected List<Path> paths;

  public UDTFConfigurations(List<Path> paths) {
    this.paths = paths;
    int seriesNumber = paths.size();
    dataPointIterationStrategies = new DataPointIterationStrategy[seriesNumber];
    dataPointBatchIterationStrategies = new DataPointBatchIterationStrategy[seriesNumber];
    tablets = new HashMap<>();
    rowRecordIterationStrategies = new HashMap<>();
    rowRecordBatchIterationStrategies = new HashMap<>();
  }


  public UDTFConfigurations setOutputDataType(TSDataType outputDataType) {
    this.outputDataType = outputDataType;
    return this;
  }


  protected DataPointIterationStrategy[] dataPointIterationStrategies;

  public UDTFConfigurations setDataPointIterationStrategy(int seriesIndex,
      DataPointIterationStrategy dataPointAccessStrategy) {
    dataPointIterationStrategies[seriesIndex] = dataPointAccessStrategy;
    return this;
  }


  protected DataPointBatchIterationStrategy[] dataPointBatchIterationStrategies;

  public UDTFConfigurations setDataPointBatchIterationStrategy(
      DataPointBatchIterationStrategy dataPointBatchIterationStrategy) {
    dataPointBatchIterationStrategies[dataPointBatchIterationStrategy
        .getSeriesIndex()] = dataPointBatchIterationStrategy;
    return this;
  }


  protected HashMap<String, List<Integer>> tablets;

  public UDTFConfigurations setRowRecordIterator(String tabletName, List<Integer> indexes) {
    tablets.put(tabletName, indexes);
    return this;
  }


  protected HashMap<String, RowRecordIterationStrategy> rowRecordIterationStrategies;

  public UDTFConfigurations setRowRecordIterationStrategy(String tabletName,
      RowRecordIterationStrategy rowRecordAccessStrategy) {
    rowRecordIterationStrategies.put(tabletName, rowRecordAccessStrategy);
    return this;
  }


  protected HashMap<String, RowRecordBatchIterationStrategy> rowRecordBatchIterationStrategies;

  public UDTFConfigurations setRowRecordBatchIterationStrategy(
      RowRecordBatchIterationStrategy rowRecordBatchIterationStrategy) {
    rowRecordBatchIterationStrategies
        .put(rowRecordBatchIterationStrategy.getTabletName(), rowRecordBatchIterationStrategy);
    return this;
  }


  public void check() throws QueryProcessException {
    super.check();

    // for data point iterators
    for (int i = 0; i < dataPointIterationStrategies.length; ++i) {
      if (dataPointIterationStrategies[i] == null) {
        continue;
      }
      switch (dataPointIterationStrategies[i]) {
        case FETCH_BY_SIZE_LIMITED_WINDOW:
        case FETCH_BY_TIME_WINDOW:
          DataPointBatchIterationStrategy dataPointBatchIterationStrategy = dataPointBatchIterationStrategies[i];
          if (dataPointBatchIterationStrategy == null) {
            throw new QueryProcessException(String.format(
                "DataPointBatchIterationStrategy is not set for corresponding data point batch iterator (seriesIndex: %d)",
                i));
          }
          dataPointBatchIterationStrategy.check();
          break;
      }
    }

    // for row record iterators
    for (String tabletName : tablets.keySet()) {
      RowRecordIterationStrategy strategy = rowRecordIterationStrategies.get(tabletName);
      if (strategy == null) {
        throw new QueryProcessException("Row record iteration strategy is not set for tablet %s.");
      }
      switch (strategy) {
        case FETCH_BY_SIZE_LIMITED_WINDOW:
        case FETCH_BY_TIME_WINDOW:
          RowRecordBatchIterationStrategy rowRecordBatchIterationStrategy = rowRecordBatchIterationStrategies
              .get(tabletName);
          if (rowRecordBatchIterationStrategy == null) {
            throw new QueryProcessException(String.format(
                "RowRecordBatchIterationStrategy is not set for corresponding row record batch iterator (tabletName: %s)",
                tabletName));
          }
          rowRecordBatchIterationStrategy.check();
          break;
      }
    }
  }


  public DataPointIterationStrategy[] getDataPointIterationStrategies() {
    return dataPointIterationStrategies;
  }

  public DataPointBatchIterationStrategy[] getDataPointBatchIterationStrategies() {
    return dataPointBatchIterationStrategies;
  }

  public HashMap<String, List<Integer>> getTablets() {
    return tablets;
  }

  public HashMap<String, RowRecordIterationStrategy> getRowRecordIterationStrategies() {
    return rowRecordIterationStrategies;
  }

  public HashMap<String, RowRecordBatchIterationStrategy> getRowRecordBatchIterationStrategies() {
    return rowRecordBatchIterationStrategies;
  }
}
