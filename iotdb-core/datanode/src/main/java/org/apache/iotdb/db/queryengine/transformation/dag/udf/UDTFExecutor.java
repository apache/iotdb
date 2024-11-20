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

package org.apache.iotdb.db.queryengine.transformation.dag.udf;

import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.adapter.PointCollectorAdaptor;
import org.apache.iotdb.db.queryengine.transformation.dag.util.InputRowUtils;
import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.udf.api.utils.RowImpl;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class UDTFExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDTFExecutor.class);

  protected final String functionName;
  protected final UDTFConfigurations configurations;

  protected UDTF udtf;

  protected ElasticSerializableTVList outputStorage;
  protected PointCollectorAdaptor collector;
  protected Column[] cachedColumns;

  protected boolean isFirstIter = true;
  protected boolean useBatch = true;

  public UDTFExecutor(String functionName, ZoneId zoneId) {
    this.functionName = functionName;
    configurations = new UDTFConfigurations(zoneId);
  }

  public void beforeStart(
      String queryId,
      float collectorMemoryBudgetInMB,
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {
    reflectAndValidateUDF(childExpressions, childExpressionDataTypes, attributes);
    configurations.check();

    // Mappable UDF does not need PointCollector
    if (!AccessStrategy.AccessStrategyType.MAPPABLE_ROW_BY_ROW.equals(
        configurations.getAccessStrategy().getAccessStrategyType())) {
      outputStorage =
          ElasticSerializableTVList.construct(
              UDFDataTypeTransformer.transformToTsDataType(configurations.getOutputDataType()),
              queryId,
              collectorMemoryBudgetInMB,
              1);
    }
  }

  private void reflectAndValidateUDF(
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {

    udtf = UDFManagementService.getInstance().reflect(functionName, UDTF.class);

    final UDFParameters parameters =
        UDFParametersFactory.buildUdfParameters(
            childExpressions, childExpressionDataTypes, attributes);

    try {
      udtf.validate(new UDFParameterValidator(parameters));
    } catch (Exception e) {
      onError("validate(UDFParameterValidator)", e);
    }

    try {
      udtf.beforeStart(parameters, configurations);
    } catch (Exception e) {
      onError("beforeStart(UDFParameters, UDTFConfigurations)", e);
    }
  }

  public void execute(Row row, boolean isCurrentRowNull) {
    try {
      // Execute UDTF
      if (isCurrentRowNull) {
        // A null row will never trigger any UDF computing
        collector.putNull(row.getTime());
      } else {
        udtf.transform(row, collector);
      }
      // Store output data
      TimeColumn timeColumn = collector.buildTimeColumn();
      Column valueColumn = collector.buildValueColumn();

      cachedColumns = new Column[] {valueColumn, timeColumn};
      outputStorage.putColumn(timeColumn, valueColumn);
    } catch (Exception e) {
      onError("transform(Row, PointCollector)", e);
    }
  }

  public void execute(
      Column[] columns, TimeColumnBuilder timeColumnBuilder, ColumnBuilder valueColumnBuilder)
      throws Exception {
    if (isFirstIter) {
      try {
        batchExecuteRow(columns, timeColumnBuilder, valueColumnBuilder);
        useBatch = true;
      } catch (UnsupportedOperationException e) {
        singleExecuteRow(columns, timeColumnBuilder, valueColumnBuilder);
        useBatch = false;
      } catch (Exception e) {
        onError("Mappable UDTF execution error", e);
      }

      isFirstIter = false;
    } else {
      try {
        if (useBatch) {
          batchExecuteRow(columns, timeColumnBuilder, valueColumnBuilder);
        } else {
          singleExecuteRow(columns, timeColumnBuilder, valueColumnBuilder);
        }
      } catch (Exception e) {
        onError("Mappable UDTF execution error", e);
      }
    }
  }

  private void singleExecuteRow(
      Column[] columns, TimeColumnBuilder timeColumnBuilder, ColumnBuilder valueColumnBuilder)
      throws Exception {
    int colCount = columns.length;
    int rowCount = columns[0].getPositionCount();

    // collect input data types from columns
    TSDataType[] dataTypes = new TSDataType[colCount];
    for (int i = 0; i < colCount; i++) {
      dataTypes[i] = columns[i].getDataType();
    }

    collector = new PointCollectorAdaptor(timeColumnBuilder, valueColumnBuilder);
    // iterate each row
    for (int i = 0; i < rowCount; i++) {
      // collect values from columns
      Object[] values = new Object[colCount];
      for (int j = 0; j < colCount; j++) {
        values[j] = columns[j].isNull(i) ? null : columns[j].getObject(i);
      }

      // construct input row for executor
      RowImpl row = new RowImpl(dataTypes);
      row.setRowRecord(values);

      boolean isAllNull = InputRowUtils.isAllNull(values);
      if (isAllNull) {
        // skip row that all fields are null
        collector.putNull(row.getTime());
      } else {
        // transform each row by default
        udtf.transform(row, collector);
      }
    }
    // Store output data
    TimeColumn timeColumn = collector.buildTimeColumn();
    Column valueColumn = collector.buildValueColumn();

    // Some UDTF only generate data in terminate method
    if (timeColumn.getPositionCount() != 0) {
      cachedColumns = new Column[] {valueColumn, timeColumn};
      outputStorage.putColumn(timeColumn, valueColumn);
    } else {
      cachedColumns = null;
    }
  }

  private void batchExecuteRow(
      Column[] columns, TimeColumnBuilder timeColumnBuilder, ColumnBuilder valueColumnBuilder)
      throws Exception {
    udtf.transform(columns, timeColumnBuilder, valueColumnBuilder);

    Column timeColumn = timeColumnBuilder.build();
    Column valueColumn = valueColumnBuilder.build();

    // Some UDTF only generate data in terminate method
    if (timeColumn.getPositionCount() != 0) {
      cachedColumns = new Column[] {valueColumn, timeColumn};
      outputStorage.putColumn((TimeColumn) timeColumn, valueColumn);
    } else {
      cachedColumns = null;
    }
  }

  public void execute(
      RowWindow rowWindow, TimeColumnBuilder timeColumnBuilder, ColumnBuilder valueColumnBuilder) {
    try {
      collector = new PointCollectorAdaptor(timeColumnBuilder, valueColumnBuilder);
      // Execute UDTF
      udtf.transform(rowWindow, collector);
      // Store output data
      TimeColumn timeColumn = collector.buildTimeColumn();
      Column valueColumn = collector.buildValueColumn();

      if (timeColumn.getPositionCount() != 0) {
        cachedColumns = new Column[] {valueColumn, timeColumn};
        outputStorage.putColumn(timeColumn, valueColumn);
      } else {
        cachedColumns = null;
      }
    } catch (Exception e) {
      onError("transform(RowWindow, PointCollector)", e);
    }
  }

  public void execute(Column[] columns, ColumnBuilder builder) {
    try {
      udtf.transform(columns, builder);
    } catch (Exception e) {
      onError("transform(TsBlock, ColumnBuilder)", e);
    }
  }

  public Column[] getCurrentBlock() {
    return cachedColumns;
  }

  public void terminate(TimeColumnBuilder timeColumnBuilder, ColumnBuilder valueColumnBuilder) {
    try {
      collector = new PointCollectorAdaptor(timeColumnBuilder, valueColumnBuilder);
      udtf.terminate(collector);

      // Some terminate method won't produce new data
      TimeColumn timeColumn = collector.buildTimeColumn();
      Column valueColumn = collector.buildValueColumn();
      if (valueColumn.getPositionCount() != 0) {
        cachedColumns = new Column[] {valueColumn, timeColumn};
        outputStorage.putColumn(timeColumn, valueColumn);
      }
    } catch (Exception e) {
      onError("terminate(PointCollector)", e);
    }
  }

  public void beforeDestroy() {
    if (udtf != null) {
      udtf.beforeDestroy();
    }
  }

  private void onError(String methodName, Exception e) {
    LOGGER.warn(
        "Error occurred during executing UDTF, please check whether the implementation of UDF is correct according to the udf-api description.",
        e);
    throw new RuntimeException(
        String.format(
                "Error occurred during executing UDTF#%s: %s, please check whether the implementation of UDF is correct according to the udf-api description.",
                methodName, System.lineSeparator())
            + e);
  }

  public UDTFConfigurations getConfigurations() {
    return configurations;
  }

  public ElasticSerializableTVList getOutputStorage() {
    return outputStorage;
  }
}
