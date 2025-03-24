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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.WindowTVFUtils.findColumnIndex;

public class FillM4 implements TableFunction {

  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String COL_PARAMETER_NAME = "COL";
  private static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String INTERVAL_PARAMETER_NAME = "INTERVAL";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .passThroughColumns()
            .build(),
        ScalarParameterSpecification.builder().name(COL_PARAMETER_NAME).type(Type.STRING).build(),
        ScalarParameterSpecification.builder()
            .name(TIMECOL_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue("time")
            .build(),
        ScalarParameterSpecification.builder()
            .name(INTERVAL_PARAMETER_NAME)
            .type(Type.INT64)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    int requiredIndex1 =
        findColumnIndex(
            tableArgument,
            (String) ((ScalarArgument) arguments.get(TIMECOL_PARAMETER_NAME)).getValue(),
            ImmutableSet.of(Type.TIMESTAMP));
    int requiredIndex2 =
        findColumnIndex(
            tableArgument,
            (String) ((ScalarArgument) arguments.get(COL_PARAMETER_NAME)).getValue(),
            ImmutableSet.of(Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE));
    DescribedSchema properColumnSchema =
        new DescribedSchema.Builder()
            .addField("fill_value", Type.DOUBLE)
            .addField("is_origin", Type.BOOLEAN)
            .build();
    // outputColumnSchema
    return TableFunctionAnalysis.builder()
        .properColumnSchema(properColumnSchema)
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, ImmutableList.of(requiredIndex1, requiredIndex2))
        .build();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(Map<String, Argument> arguments) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        long interval = (long) ((ScalarArgument) arguments.get(INTERVAL_PARAMETER_NAME)).getValue();
        return new FillM4Processor(interval);
      }
    };
  }

  private static class FillM4Processor implements TableFunctionDataProcessor {

    private final long interval;
    private M4Data m4Data = null;
    private long windowEnd = -1;
    private long curIndex = 0;

    public FillM4Processor(long interval) {
      this.interval = interval;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      long timeValue = input.getLong(0);
      if (m4Data != null && timeValue > windowEnd) {
        outputWindow(properColumnBuilders, passThroughIndexBuilder);
      }
      if (m4Data == null) {
        m4Data = new M4Data(input.getDouble(1), curIndex);
        windowEnd = timeValue + interval;
      }
      m4Data.update(input.getDouble(1), curIndex);
      curIndex++;
    }

    @Override
    public void finish(List<ColumnBuilder> columnBuilders, ColumnBuilder passThroughIndexBuilder) {
      if (m4Data != null) {
        outputWindow(columnBuilders, passThroughIndexBuilder);
      }
    }

    private void outputWindow(
        List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
      // 对 M4 数据去重后输出
      List<Pair<Long, Double>> m4DataList =
          Arrays.asList(
              new Pair<>(m4Data.firstIndex, m4Data.firstValue),
              new Pair<>(m4Data.minIndex, m4Data.minValue),
              new Pair<>(m4Data.maxIndex, m4Data.maxValue),
              new Pair<>(m4Data.lastIndex, m4Data.lastValue));
      m4DataList.sort((o1, o2) -> (int) (o1.left - o2.left));
      properColumnBuilders.get(0).writeDouble(m4DataList.get(0).right);
      properColumnBuilders.get(1).writeBoolean(true);
      passThroughIndexBuilder.writeLong(m4DataList.get(0).left);
      for (int i = 1; i < m4DataList.size(); i++) {
        if (!m4DataList.get(i).left.equals(m4DataList.get(i - 1).left)) {
          properColumnBuilders.get(0).writeDouble(m4DataList.get(i).right);
          properColumnBuilders.get(1).writeBoolean(true);
          passThroughIndexBuilder.writeLong(m4DataList.get(i).left);
        }
      }
      m4Data = null;
    }
  }

  private static class M4Data {
    private double firstValue;
    private long firstIndex;
    private double minValue;
    private long minIndex;
    private long maxIndex;
    private double maxValue;
    private double lastValue;
    private long lastIndex;

    public M4Data(double value, long index) {
      this.firstValue = value;
      this.minValue = value;
      this.maxValue = value;
      this.lastValue = value;
      this.firstIndex = index;
      this.minIndex = index;
      this.maxIndex = index;
      this.lastIndex = index;
    }

    public void update(double value, long index) {
      if (value < maxValue) {
        minValue = value;
        minIndex = index;
      }
      if (value > maxValue) {
        maxValue = value;
        maxIndex = index;
      }
      lastValue = value;
      lastIndex = index;
    }
  }
}
