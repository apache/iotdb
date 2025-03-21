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
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class GroupBySession implements TableFunction {
  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String GAP_PARAMETER_NAME = "GAP";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(DATA_PARAMETER_NAME).build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
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
            .addField("count", Type.INT64)
            .build();

    // outputColumnSchema
    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchema)
        .requiredColumns(DATA_PARAMETER_NAME, Collections.singletonList(requiredIndex))
        .build();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(Map<String, Argument> arguments) {
    long gap = (long) ((ScalarArgument) arguments.get(GAP_PARAMETER_NAME)).getValue();
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new SessionDataProcessor(gap);
      }
    };
  }

  private static class SessionDataProcessor implements TableFunctionDataProcessor {

    private final long gap;
    private long currentRowIndex = 0;
    private long count = 0;
    private long curIndex = 0;
    private long windowStart = -1;
    private long windowEnd = -1;

    public SessionDataProcessor(long gap) {
      this.gap = gap;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      long timeValue = input.getLong(0);
      if (currentRowIndex != 0 && timeValue > windowEnd) {
        outputWindow(properColumnBuilders, passThroughIndexBuilder);
      }
      if (count == 0) {
        windowStart = timeValue;
        currentRowIndex = curIndex;
      }
      windowEnd = timeValue + gap;
      curIndex++;
      count++;
    }

    @Override
    public void process(
        Column[] inputs,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      Column timeColumn = inputs[0];
      for (int i = 0; i < timeColumn.getPositionCount(); i++) {
        long timeValue = timeColumn.getLong(0);
        if (currentRowIndex != 0 && timeValue > windowEnd) {
          outputWindow(properColumnBuilders, passThroughIndexBuilder);
        }
        if (count == 0) {
          windowStart = timeValue;
          currentRowIndex = curIndex;
        }
        windowEnd = timeValue + gap;
        curIndex++;
        count++;
      }
    }

    @Override
    public void finish(List<ColumnBuilder> columnBuilders, ColumnBuilder passThroughIndexBuilder) {
      if (count != 0) {
        outputWindow(columnBuilders, passThroughIndexBuilder);
      }
    }

    private void outputWindow(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      properColumnBuilders.get(0).writeLong(windowStart);
      properColumnBuilders.get(1).writeLong(windowEnd - gap);
      properColumnBuilders.get(2).writeLong(count);
      passThroughIndexBuilder.writeLong(currentRowIndex);
      count = 0;
    }
  }
}
