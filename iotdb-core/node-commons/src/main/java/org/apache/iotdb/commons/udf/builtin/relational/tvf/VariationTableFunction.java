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
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class VariationTableFunction implements TableFunction {
  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String COL_PARAMETER_NAME = "COL";
  private static final String DELTA_PARAMETER_NAME = "DELTA";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .passThroughColumns()
            .build(),
        ScalarParameterSpecification.builder().name(COL_PARAMETER_NAME).type(Type.STRING).build(),
        ScalarParameterSpecification.builder()
            .name(DELTA_PARAMETER_NAME)
            .type(Type.DOUBLE)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    String expectedFieldName =
        (String) ((ScalarArgument) arguments.get(COL_PARAMETER_NAME)).getValue();
    int requiredIndex =
        findColumnIndex(
            tableArgument,
            expectedFieldName,
            ImmutableSet.of(Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE));
    DescribedSchema properColumnSchema =
        new DescribedSchema.Builder().addField("window_index", Type.INT64).build();
    // outputColumnSchema
    MapTableFunctionHandle handle = new MapTableFunctionHandle();
    handle.addProperty(
        DELTA_PARAMETER_NAME, ((ScalarArgument) arguments.get(DELTA_PARAMETER_NAME)).getValue());
    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchema)
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, Collections.singletonList(requiredIndex))
        .handle(handle)
        .build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new MapTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    double delta =
        (double) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(DELTA_PARAMETER_NAME);
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new VariationDataProcessor(delta);
      }
    };
  }

  private static class VariationDataProcessor implements TableFunctionDataProcessor {

    private final double gap;
    private long currentStartIndex = -1;
    private double baseValue = 0;
    private long curIndex = 0;
    private long windowIndex = 0;

    public VariationDataProcessor(double delta) {
      this.gap = delta;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      double value = input.getDouble(0);
      if (currentStartIndex == -1) {
        // init the first window
        currentStartIndex = curIndex;
        baseValue = value;
      } else if (Math.abs(value - baseValue) > gap) {
        outputWindow(properColumnBuilders, passThroughIndexBuilder);
        currentStartIndex = curIndex;
        // use the first value in the window as the base value
        baseValue = value;
      }
      curIndex++;
    }

    @Override
    public void finish(List<ColumnBuilder> columnBuilders, ColumnBuilder passThroughIndexBuilder) {
      outputWindow(columnBuilders, passThroughIndexBuilder);
    }

    private void outputWindow(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      for (long i = currentStartIndex; i < curIndex; i++) {
        properColumnBuilders.get(0).writeLong(windowIndex);
        passThroughIndexBuilder.writeLong(i);
      }
      windowIndex++;
    }
  }
}
