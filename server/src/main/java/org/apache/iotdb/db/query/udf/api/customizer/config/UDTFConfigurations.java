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
import org.apache.iotdb.db.query.udf.api.customizer.strategy.DataPointWindowIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.DataPointIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowRecordWindowIterationStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowRecordIterationStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public class UDTFConfigurations extends UDFConfigurations {

  protected List<Path> paths;

  public UDTFConfigurations(List<Path> paths) {
    this.paths = paths;
    int seriesNumber = paths.size();
    dataPointIterationStrategies = new DataPointIterationStrategy[seriesNumber];
    dataPointWindowIterationStrategies = new DataPointWindowIterationStrategy[seriesNumber];
    tablets = new HashMap<>();
    rowRecordIterationStrategies = new HashMap<>();
    rowRecordWindowIterationStrategies = new HashMap<>();
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


  protected DataPointWindowIterationStrategy[] dataPointWindowIterationStrategies;

  public UDTFConfigurations setDataPointWindowIterationStrategy(
      DataPointWindowIterationStrategy dataPointWindowIterationStrategy) {
    dataPointWindowIterationStrategies[dataPointWindowIterationStrategy
        .getSeriesIndex()] = dataPointWindowIterationStrategy;
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


  protected HashMap<String, RowRecordWindowIterationStrategy> rowRecordWindowIterationStrategies;

  public UDTFConfigurations setRowRecordWindowIterationStrategy(
      RowRecordWindowIterationStrategy rowRecordWindowIterationStrategy) {
    rowRecordWindowIterationStrategies
        .put(rowRecordWindowIterationStrategy.getTabletName(), rowRecordWindowIterationStrategy);
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
        case FETCH_BY_TUMBLING_TIME_WINDOW:
        case FETCH_BY_SLIDING_TIME_WINDOW:
          DataPointWindowIterationStrategy dataPointWindowIterationStrategy = dataPointWindowIterationStrategies[i];
          if (dataPointWindowIterationStrategy == null) {
            throw new QueryProcessException(String.format(
                "DataPointWindowIterationStrategy is not set for corresponding data point window iterator (seriesIndex: %d)",
                i));
          }
          dataPointWindowIterationStrategy.check();
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
        case FETCH_BY_TUMBLING_TIME_WINDOW:
        case FETCH_BY_SLIDING_TIME_WINDOW:
          RowRecordWindowIterationStrategy rowRecordWindowIterationStrategy = rowRecordWindowIterationStrategies
              .get(tabletName);
          if (rowRecordWindowIterationStrategy == null) {
            throw new QueryProcessException(String.format(
                "RowRecordWindowIterationStrategy is not set for corresponding row record window iterator (tabletName: %s)",
                tabletName));
          }
          rowRecordWindowIterationStrategy.check();
          break;
      }
    }
  }


  public DataPointIterationStrategy[] getDataPointIterationStrategies() {
    return dataPointIterationStrategies;
  }

  public DataPointWindowIterationStrategy[] getDataPointWindowIterationStrategies() {
    return dataPointWindowIterationStrategies;
  }

  public HashMap<String, List<Integer>> getTablets() {
    return tablets;
  }

  public HashMap<String, RowRecordIterationStrategy> getRowRecordIterationStrategies() {
    return rowRecordIterationStrategies;
  }

  public HashMap<String, RowRecordWindowIterationStrategy> getRowRecordWindowIterationStrategies() {
    return rowRecordWindowIterationStrategies;
  }
}
