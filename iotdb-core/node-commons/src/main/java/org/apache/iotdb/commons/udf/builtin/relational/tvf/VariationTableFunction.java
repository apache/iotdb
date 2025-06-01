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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class VariationTableFunction implements TableFunction {
  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String COL_PARAMETER_NAME = "COL";
  private static final String DELTA_PARAMETER_NAME = "DELTA";
  private static final String IGNORE_NULL_PARAMETER_NAME = "IGNORENULL";

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
            .type(Type.DOUBLE).defaultValue(0.0)
            .build(),
        ScalarParameterSpecification.builder().name(IGNORE_NULL_PARAMETER_NAME).type(Type.BOOLEAN).defaultValue(false).build());
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
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(
                DELTA_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(DELTA_PARAMETER_NAME)).getValue())
                .addProperty(IGNORE_NULL_PARAMETER_NAME,
                    ((ScalarArgument) arguments.get(IGNORE_NULL_PARAMETER_NAME)).getValue())
            .build();
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
    boolean ignoreNull = (boolean) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(IGNORE_NULL_PARAMETER_NAME);
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new VariationDataProcessor(delta, ignoreNull);
      }
    };
  }

  private static class VariationDataProcessor implements TableFunctionDataProcessor {

    private final double gap;
    private final boolean ignoreNull;
    private final Queue<Long> skipIndex = new LinkedList<>();

    private long currentStartIndex = 0;
    private double baseValue = 0;
    private long curIndex = 0;
    private long windowIndex = 0;
    private boolean previousIsNull = true;

    public VariationDataProcessor(double delta, boolean ignoreNull) {
      this.gap = delta;
      this.ignoreNull = ignoreNull;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      if(input.isNull(0)){
        // handle null value
        if(ignoreNull) {
          // skip null values
          skipIndex.add(curIndex);
        }else if(!previousIsNull) {
          // output window and reset currentStartIndex
          outputWindow(properColumnBuilders, passThroughIndexBuilder);
          currentStartIndex = curIndex;
          previousIsNull = true;
        }
      }else{
        double value = input.getDouble(0);
        if (previousIsNull||Math.abs(value - baseValue) > gap) {
          outputWindow(properColumnBuilders, passThroughIndexBuilder);
          currentStartIndex = curIndex;
          // use the first value in the window as the base value
          baseValue = value;
        }
        previousIsNull = false;
      }

      curIndex++;
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      outputWindow(properColumnBuilders, passThroughIndexBuilder);
    }

    private void outputWindow(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      boolean increaseIndex = false;
      for (long i = currentStartIndex; i < curIndex; i++) {
        if(!skipIndex.isEmpty()&&i==skipIndex.peek()){
          // skip the index if it is in the skip queue
          skipIndex.poll();
          continue;
        }
        properColumnBuilders.get(0).writeLong(windowIndex);
        passThroughIndexBuilder.writeLong(i);
        increaseIndex = true;
      }
      windowIndex += increaseIndex ? 1 : 0;
    }
  }
}
