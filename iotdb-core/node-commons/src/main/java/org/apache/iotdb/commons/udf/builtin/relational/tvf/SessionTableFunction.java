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

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class SessionTableFunction implements TableFunction {
  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String GAP_PARAMETER_NAME = "GAP";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .passThroughColumns()
            .build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue("time")
            .build(),
        ScalarParameterSpecification.builder().name(GAP_PARAMETER_NAME).type(Type.INT64).build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    String expectedFieldName =
        (String) ((ScalarArgument) arguments.get(TIMECOL_PARAMETER_NAME)).getValue();
    int requiredIndex =
        findColumnIndex(tableArgument, expectedFieldName, Collections.singleton(Type.TIMESTAMP));
    DescribedSchema properColumnSchema =
        new DescribedSchema.Builder()
            .addField("window_start", Type.TIMESTAMP)
            .addField("window_end", Type.TIMESTAMP)
            .build();

    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(
                GAP_PARAMETER_NAME, ((ScalarArgument) arguments.get(GAP_PARAMETER_NAME)).getValue())
            .build();
    // outputColumnSchema
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
    long gap =
        (long) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(GAP_PARAMETER_NAME);
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new SessionDataProcessor(gap);
      }
    };
  }

  private static class SessionDataProcessor implements TableFunctionDataProcessor {

    private final long gap;
    private long currentStartIndex = 0;
    private long curIndex = 0;
    private long windowStart = Long.MIN_VALUE;
    private long windowEnd = Long.MIN_VALUE;

    public SessionDataProcessor(long gap) {
      this.gap = gap;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      long timeValue = input.getLong(0);
      if (timeValue > windowEnd) {
        outputWindow(properColumnBuilders, passThroughIndexBuilder);
        currentStartIndex = curIndex;
        windowStart = timeValue;
      }
      windowEnd = timeValue + gap;
      curIndex++;
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      outputWindow(properColumnBuilders, passThroughIndexBuilder);
    }

    private void outputWindow(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      long currentWindowEnd = windowEnd - gap;
      for (long i = currentStartIndex; i < curIndex; i++) {
        properColumnBuilders.get(0).writeLong(windowStart);
        properColumnBuilders.get(1).writeLong(currentWindowEnd);
        passThroughIndexBuilder.writeLong(i);
      }
    }
  }
}
