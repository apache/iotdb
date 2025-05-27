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
import static org.apache.iotdb.udf.api.relational.table.argument.ScalarArgumentChecker.POSITIVE_LONG_CHECKER;

public class CumulateTableFunction implements TableFunction {

  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String SIZE_PARAMETER_NAME = "SIZE";
  private static final String STEP_PARAMETER_NAME = "STEP";
  private static final String ORIGIN_PARAMETER_NAME = "ORIGIN";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .rowSemantics()
            .passThroughColumns()
            .build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue("time")
            .build(),
        ScalarParameterSpecification.builder()
            .name(SIZE_PARAMETER_NAME)
            .type(Type.INT64)
            .addChecker(POSITIVE_LONG_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(STEP_PARAMETER_NAME)
            .type(Type.INT64)
            .addChecker(POSITIVE_LONG_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(ORIGIN_PARAMETER_NAME)
            .type(Type.TIMESTAMP)
            .defaultValue(0L)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    // size must be an integral multiple of step.
    long size = (long) ((ScalarArgument) arguments.get(SIZE_PARAMETER_NAME)).getValue();
    long step = (long) ((ScalarArgument) arguments.get(STEP_PARAMETER_NAME)).getValue();

    if (size % step != 0) {
      throw new UDFException(
          "Cumulative table function requires size must be an integral multiple of step.");
    }

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
            .addProperty(STEP_PARAMETER_NAME, step)
            .addProperty(SIZE_PARAMETER_NAME, size)
            .addProperty(
                ORIGIN_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(ORIGIN_PARAMETER_NAME)).getValue())
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
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new CumulateDataProcessor(
            (Long)
                ((MapTableFunctionHandle) tableFunctionHandle).getProperty(ORIGIN_PARAMETER_NAME),
            (Long) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(STEP_PARAMETER_NAME),
            (Long) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(SIZE_PARAMETER_NAME));
      }
    };
  }

  private static class CumulateDataProcessor implements TableFunctionDataProcessor {

    private final long step;
    private final long size;
    private final long origin;
    private long curIndex = 0;

    public CumulateDataProcessor(long startTime, long step, long size) {
      this.step = step;
      this.size = size;
      this.origin = startTime;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      // find the first windows
      long timeValue = input.getLong(0);
      if (timeValue >= origin) {
        long windowStart = origin + (timeValue - origin) / size * size;
        for (long steps = (timeValue - windowStart + step) / step * step;
            steps <= size;
            steps += step) {
          properColumnBuilders.get(0).writeLong(windowStart);
          properColumnBuilders.get(1).writeLong(windowStart + steps);
          passThroughIndexBuilder.writeLong(curIndex);
        }
      }
      curIndex++;
    }
  }
}
