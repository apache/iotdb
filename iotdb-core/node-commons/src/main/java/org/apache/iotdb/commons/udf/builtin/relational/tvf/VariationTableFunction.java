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
  private static final String IGNORE_NULL_PARAMETER_NAME = "IGNORE_NULL";

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
            .defaultValue(true)
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
    void resetBaseValue(Record input) {
      if (input.isNull(0)) {
        baseValue = null;
      } else {
        baseValue = input.getObject(0);
      }
    }

    @Override
    boolean isCurrentBaseValueIsNull() {
      return baseValue == null;
    }

    @Override
    boolean outOfBound(Record input) {
      if (isCurrentBaseValueIsNull()) {
        throw new IllegalStateException("When comparing, base value should never be null");
      }
      return !input.getObject(0).equals(baseValue);
    }
  }

  private static class NonEquivalentVariationDataProcessor extends VariationDataProcessor {
    private final double gap;
    private double baseValue = 0;
    private boolean baseValueIsNull = true;

    public NonEquivalentVariationDataProcessor(double delta, boolean ignoreNull) {
      super(ignoreNull);
      this.gap = delta;
    }

    @Override
    void resetBaseValue(Record input) {
      if (input.isNull(0)) {
        baseValueIsNull = true;
      } else {
        baseValueIsNull = false;
        baseValue = input.getDouble(0);
      }
    }

    @Override
    boolean isCurrentBaseValueIsNull() {
      return baseValueIsNull;
    }

    @Override
    boolean outOfBound(Record input) {
      if (isCurrentBaseValueIsNull()) {
        throw new IllegalStateException("When comparing, base value should never be null");
      }
      return Math.abs(input.getDouble(0) - baseValue) > gap;
    }
  }

  private abstract static class VariationDataProcessor implements TableFunctionDataProcessor {

    protected final boolean ignoreNull;

    // first row index of current window
    protected long currentStartIndex = 0;
    // current row index
    protected long curIndex = 0;
    // current window number
    private long windowIndex = 0;

    public VariationDataProcessor(boolean ignoreNull) {
      this.ignoreNull = ignoreNull;
    }

    // reset base value anyway
    abstract void resetBaseValue(Record input);

    abstract boolean isCurrentBaseValueIsNull();

    // reset base value only if its current value is null
    void resetBaseValueIfNull(Record input) {
      if (isCurrentBaseValueIsNull()) {
        resetBaseValue(input);
      }
    }

    boolean isNewGroup(Record input) {
      if (input.isNull(0)) { // current row is null
        if (isCurrentBaseValueIsNull()) { // current base value of current group is also null
          // in such case, whether ignoreNull is true or false, current row always belongs to
          // current group
          return false;
        } else { // current base value is not null
          // ignoreNull is true, current null row belongs to current group
          // ignoreNull is false, current null row doesn't belong to current group
          return !ignoreNull;
        }
      } else { // current row is not null
        if (isCurrentBaseValueIsNull()) { // current base value of current group is null
          // ignoreNull is true, current non-null row belongs to current group
          // ignoreNull is false, current non-null row doesn't belong to current group
          return !ignoreNull;
        } else { // current base value is not null
          // compare current row with the current non-null base value
          // if the diff between current row and base value exceed threshold, current non-null row
          // doesn't belong to current group
          // if the diff within the threshold, current non-null row belongs to current group
          return outOfBound(input);
        }
      }
    }

    abstract boolean outOfBound(Record input);

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      if (curIndex == 0) { // first row, init state
        resetBaseValue(input);
      } else { // not first row
        if (isNewGroup(input)) { // current row doesn't belong to current group
          outputWindow(properColumnBuilders, passThroughIndexBuilder, input);
        } else { // current row belongs to current group
          // reset base value of current group if it is null
          resetBaseValueIfNull(input);
        }
      }
      curIndex++;
    }

    @Override
    public void finish(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      outputWindow(properColumnBuilders, passThroughIndexBuilder, null);
    }

    protected void outputWindow(
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder,
        Record input) {
      for (long i = currentStartIndex; i < curIndex; i++) {
        properColumnBuilders.get(0).writeLong(windowIndex);
        passThroughIndexBuilder.writeLong(i);
      }
      if (curIndex > currentStartIndex) {
        // reset if not empty group
        windowIndex++;
        currentStartIndex = curIndex;
        if (input != null) {
          resetBaseValue(input);
        }
      }
    }
  }
}
