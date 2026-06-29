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

public class HOPTableFunction implements TableFunction {

  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String SIZE_PARAMETER_NAME = "SIZE";
  private static final String SLIDE_PARAMETER_NAME = "SLIDE";
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
            .addChecker(POSITIVE_LONG_CHECKER)
            .type(Type.INT64)
            .build(),
        ScalarParameterSpecification.builder()
            .name(SLIDE_PARAMETER_NAME)
            .addChecker(POSITIVE_LONG_CHECKER)
            .type(Type.INT64)
            .build(),
        ScalarParameterSpecification.builder()
            .name(ORIGIN_PARAMETER_NAME)
            .type(Type.TIMESTAMP)
            .defaultValue(0L)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) {
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
                ORIGIN_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(ORIGIN_PARAMETER_NAME)).getValue())
            .addProperty(
                SLIDE_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(SLIDE_PARAMETER_NAME)).getValue())
            .addProperty(
                SIZE_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(SIZE_PARAMETER_NAME)).getValue())
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
    MapTableFunctionHandle mapTableFunctionHandle = (MapTableFunctionHandle) tableFunctionHandle;
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new HOPDataProcessor(
            (Long) mapTableFunctionHandle.getProperty(ORIGIN_PARAMETER_NAME),
            (Long) mapTableFunctionHandle.getProperty(SLIDE_PARAMETER_NAME),
            (Long) mapTableFunctionHandle.getProperty(SIZE_PARAMETER_NAME));
      }
    };
  }

  private static class HOPDataProcessor implements TableFunctionDataProcessor {

    private final long slide;
    private final long size;
    private final long origin;
    private long curIndex = 0;

    public HOPDataProcessor(long startTime, long slide, long size) {
      this.slide = slide;
      this.size = size;
      this.origin = startTime;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      // find the first windows that satisfy the condition: start + n*slide <= time < start +
      // n*slide + size
      long timeValue = input.getLong(0);
      if (timeValue >= origin) {
        long window_start = origin + (timeValue - origin - size + slide) / slide * slide;
        while (window_start <= timeValue && window_start + size > timeValue) {
          properColumnBuilders.get(0).writeLong(window_start);
          properColumnBuilders.get(1).writeLong(window_start + size);
          passThroughIndexBuilder.writeLong(curIndex);
          window_start += slide;
        }
      }
      curIndex++;
    }
  }
}
