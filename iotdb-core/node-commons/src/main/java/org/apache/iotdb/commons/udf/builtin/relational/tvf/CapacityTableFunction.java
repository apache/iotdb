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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CapacityTableFunction implements TableFunction {
  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String SIZE_PARAMETER_NAME = "SIZE";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .passThroughColumns()
            .build(),
        ScalarParameterSpecification.builder().name(SIZE_PARAMETER_NAME).type(Type.INT64).build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    long size = (long) ((ScalarArgument) arguments.get(SIZE_PARAMETER_NAME)).getValue();
    if (size <= 0) {
      throw new UDFException("Size must be greater than 0");
    }
    return TableFunctionAnalysis.builder()
        .properColumnSchema(
            new DescribedSchema.Builder().addField("window_index", Type.INT64).build())
        .requiredColumns(DATA_PARAMETER_NAME, Collections.singletonList(0))
        .build();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(Map<String, Argument> arguments) {
    long sz = (long) ((ScalarArgument) arguments.get(SIZE_PARAMETER_NAME)).getValue();
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new CountDataProcessor(sz);
      }
    };
  }

  private static class CountDataProcessor implements TableFunctionDataProcessor {

    private final long size;
    private final List<Long> currentRowIndexes = new ArrayList<>();
    private long curIndex = 0;
    private long windowIndex = 0;

    public CountDataProcessor(long size) {
      this.size = size;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      if (currentRowIndexes.size() >= size) {
        outputWindow(properColumnBuilders, passThroughIndexBuilder);
      }
      currentRowIndexes.add(curIndex);
      curIndex++;
    }

    @Override
    public void finish(List<ColumnBuilder> columnBuilders, ColumnBuilder passThroughIndexBuilder) {
      if (!currentRowIndexes.isEmpty()) {
        outputWindow(columnBuilders, passThroughIndexBuilder);
      }
    }

    private void outputWindow(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      for (Long currentRowIndex : currentRowIndexes) {
        properColumnBuilders.get(0).writeLong(windowIndex);
        passThroughIndexBuilder.writeLong(currentRowIndex);
      }
      windowIndex++;
      currentRowIndexes.clear();
    }
  }
}
