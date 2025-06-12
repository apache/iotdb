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
import org.apache.iotdb.udf.api.exception.UDFTypeMismatchException;
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
import static org.apache.iotdb.udf.api.relational.table.argument.ScalarArgumentChecker.NON_NEGATIVE_DOUBLE_CHECKER;

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
            .type(Type.DOUBLE)
            .defaultValue(0.0)
            .addChecker(NON_NEGATIVE_DOUBLE_CHECKER)
            .build(),
        ScalarParameterSpecification.builder()
            .name(IGNORE_NULL_PARAMETER_NAME)
            .type(Type.BOOLEAN)
            .defaultValue(false)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    String expectedFieldName =
        (String) ((ScalarArgument) arguments.get(COL_PARAMETER_NAME)).getValue();
    double delta = (double) ((ScalarArgument) arguments.get(DELTA_PARAMETER_NAME)).getValue();
    int requiredIndex;
    try {
      requiredIndex =
          findColumnIndex(
              tableArgument,
              expectedFieldName,
              delta == 0
                  ? ImmutableSet.copyOf(Type.allTypes())
                  : ImmutableSet.copyOf(Type.numericTypes()));
    } catch (UDFTypeMismatchException e) {
      // print more information for the exception
      throw new UDFTypeMismatchException(
          e.getMessage() + " The column type must be numeric if DELTA is not 0.", e);
    }

    DescribedSchema properColumnSchema =
        new DescribedSchema.Builder().addField("window_index", Type.INT64).build();
    // outputColumnSchema
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(DELTA_PARAMETER_NAME, delta)
            .addProperty(
                IGNORE_NULL_PARAMETER_NAME,
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
    boolean ignoreNull =
        (boolean)
            ((MapTableFunctionHandle) tableFunctionHandle).getProperty(IGNORE_NULL_PARAMETER_NAME);
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return delta == 0
            ? new EquivalentVariationDataProcessor(ignoreNull)
            : new NonEquivalentVariationDataProcessor(delta, ignoreNull);
      }
    };
  }

  private static class EquivalentVariationDataProcessor extends VariationDataProcessor {
    protected Object baseValue = null;

    public EquivalentVariationDataProcessor(boolean ignoreNull) {
      super(ignoreNull);
    }

    @Override
    void resetBaseValue(Record input, List<ColumnBuilder> properColumnBuilders) {
      baseValue = input.getObject(0);
    }

    @Override
    boolean isNewGroup(Record input, List<ColumnBuilder> properColumnBuilders) {
      return !input.getObject(0).equals(baseValue);
    }
  }

  private static class NonEquivalentVariationDataProcessor extends VariationDataProcessor {
    private final double gap;
    private double baseValue = 0;

    public NonEquivalentVariationDataProcessor(double delta, boolean ignoreNull) {
      super(ignoreNull);
      this.gap = delta;
    }

    @Override
    void resetBaseValue(Record input, List<ColumnBuilder> properColumnBuilders) {
      baseValue = input.getDouble(0);
    }

    @Override
    boolean isNewGroup(Record input, List<ColumnBuilder> properColumnBuilders) {
      return Math.abs(input.getDouble(0) - baseValue) > gap;
    }
  }

  private abstract static class VariationDataProcessor implements TableFunctionDataProcessor {

    private final boolean ignoreNull;

    protected long currentStartIndex = 0;
    protected long curIndex = 0;
    protected boolean currentGroupContainsNonNull = false;
    protected boolean currentGroupContainsNull = false;
    private long windowIndex = 0;

    public VariationDataProcessor(boolean ignoreNull) {
      this.ignoreNull = ignoreNull;
    }

    abstract void resetBaseValue(Record input, List<ColumnBuilder> properColumnBuilders);

    abstract boolean isNewGroup(Record input, List<ColumnBuilder> properColumnBuilders);

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      if (input.isNull(0)) {
        // handle null value
        if (!ignoreNull && currentGroupContainsNonNull) {
          outputWindow(properColumnBuilders, passThroughIndexBuilder);
        }
        currentGroupContainsNull = true;
      } else {
        // handle non-null value
        boolean shouldOutput = false;
        if (!ignoreNull && currentGroupContainsNull) {
          // null is a special value when IgnoreNull is false.
          // ==> Must output if current group contains null value.
          shouldOutput = true;
        } else if (!currentGroupContainsNonNull) {
          resetBaseValue(input, properColumnBuilders);
        } else {
          // ==> Should output if base value is initialized and new group is detected
          shouldOutput = isNewGroup(input, properColumnBuilders);
        }
        if (shouldOutput) {
          outputWindow(properColumnBuilders, passThroughIndexBuilder);
          resetBaseValue(input, properColumnBuilders);
        }
        currentGroupContainsNonNull = true;
      }
      curIndex++;
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      outputWindow(properColumnBuilders, passThroughIndexBuilder);
    }

    protected void outputWindow(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      for (long i = currentStartIndex; i < curIndex; i++) {
        properColumnBuilders.get(0).writeLong(windowIndex);
        passThroughIndexBuilder.writeLong(i);
      }
      if (curIndex > currentStartIndex) {
        // reset if not empty group
        windowIndex++;
        currentGroupContainsNonNull = false;
        currentGroupContainsNull = false;
        currentStartIndex = curIndex;
      }
    }
  }
}
