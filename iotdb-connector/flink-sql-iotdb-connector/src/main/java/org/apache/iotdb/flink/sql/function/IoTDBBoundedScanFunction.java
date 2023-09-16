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
package org.apache.iotdb.flink.sql.function;

import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.exception.IllegalSchemaException;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.List;

public class IoTDBBoundedScanFunction extends RichInputFormat<RowData, InputSplit> {
  private final ReadableConfig options;
  private final String sql;
  private final List<Tuple2<String, DataType>> tableSchema;
  private final long lowerBound;
  private final long upperBound;
  private Session session;
  private SessionDataSet dataSet;
  private List<String> columnNames;

  public IoTDBBoundedScanFunction(ReadableConfig options, SchemaWrapper schemaWrapper) {
    this.options = options;
    tableSchema = schemaWrapper.getSchema();
    sql = options.get(Options.SQL);
    lowerBound = options.get(Options.SCAN_BOUNDED_LOWER_BOUND);
    upperBound = options.get(Options.SCAN_BOUNDED_UPPER_BOUND);
  }

  @Override
  public void configure(Configuration configuration) {
    // fo nothing
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics baseStatistics) {
    return baseStatistics;
  }

  @Override
  public InputSplit[] createInputSplits(int i) {
    return new GenericInputSplit[] {new GenericInputSplit(1, 1)};
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  @Override
  public void openInputFormat() {
    session =
        new Session.Builder()
            .nodeUrls(Arrays.asList(options.get(Options.NODE_URLS).split(",")))
            .username(options.get(Options.USER))
            .password(options.get(Options.PASSWORD))
            .build();

    try {
      session.open(false);
    } catch (IoTDBConnectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void open(InputSplit inputSplit) {
    String sql;
    if (lowerBound < 0L && upperBound < 0L) {
      sql = this.sql;
    } else if (lowerBound < 0L && upperBound > 0L) {
      sql = String.format("%s WHERE TIME <= %d", this.sql, upperBound);
    } else if (lowerBound > 0L && upperBound < 0L) {
      sql = String.format("%s WHERE TIME >= %d", this.sql, lowerBound);
    } else {
      sql = String.format("%s WHERE TIME >= %d AND TIME <= %d", this.sql, lowerBound, upperBound);
    }
    try {
      dataSet = session.executeQueryStatement(sql);
      columnNames = dataSet.getColumnNames();

      for (Tuple2<String, DataType> field : tableSchema) {
        if (!columnNames.contains(field.f0)) {
          continue;
        }
        int index = columnNames.indexOf(field.f0);
        TSDataType iotdbType = TSDataType.valueOf(dataSet.getColumnTypes().get(index));
        DataType flinkType = field.f1;
        if (!Utils.isTypeEqual(iotdbType, flinkType)) {
          throw new IllegalSchemaException(
              String.format(
                  "The data type of column `%s` is different in IoTDB and Flink", field.f0));
        }
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean reachedEnd() {
    try {
      return !dataSet.hasNext();
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RowData nextRecord(RowData rowData) {
    try {
      RowRecord rowRecord = dataSet.next();
      return Utils.convert(rowRecord, columnNames, tableSchema);
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      if (dataSet != null) {
        dataSet.close();
      }
      if (session != null) {
        session.close();
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
