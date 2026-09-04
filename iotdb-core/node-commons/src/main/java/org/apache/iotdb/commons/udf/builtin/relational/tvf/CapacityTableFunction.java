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

import org.apache.iotdb.commons.i18n.CommonMessages;
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

import static org.apache.iotdb.udf.api.relational.table.argument.ScalarArgumentChecker.POSITIVE_LONG_CHECKER;

public class CapacityTableFunction implements TableFunction {
  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String SIZE_PARAMETER_NAME = "SIZE";
  private static final String SLIDE_PARAMETER_NAME = "SLIDE";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .passThroughColumns()
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
            .defaultValue(-1L)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    long size = (long) ((ScalarArgument) arguments.get(SIZE_PARAMETER_NAME)).getValue();
    if (size <= 0) {
      throw new UDFException(CommonMessages.SIZE_MUST_BE_POSITIVE);
    }
    long slide = (long) ((ScalarArgument) arguments.get(SLIDE_PARAMETER_NAME)).getValue();
    // default SLIDE to SIZE when not specified (sentinel value -1)
    if (slide == -1L) {
      slide = size;
    }
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(SIZE_PARAMETER_NAME, size)
            .addProperty(SLIDE_PARAMETER_NAME, slide)
            .build();
    return TableFunctionAnalysis.builder()
        .properColumnSchema(
            new DescribedSchema.Builder().addField("window_index", Type.INT64).build())
        .requireRecordSnapshot(false)
        .requiredColumns(DATA_PARAMETER_NAME, Collections.singletonList(0))
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
    MapTableFunctionHandle handle = (MapTableFunctionHandle) tableFunctionHandle;
    long size = (long) handle.getProperty(SIZE_PARAMETER_NAME);
    long slide = (long) handle.getProperty(SLIDE_PARAMETER_NAME);
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new CapacityDataProcessor(size, slide);
      }
    };
  }

  private static class CapacityDataProcessor implements TableFunctionDataProcessor {

    private final long size;
    private final long slide;
    private long curIndex = 0;

    public CapacityDataProcessor(long size, long slide) {
      this.size = size;
      this.slide = slide;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      // For each row at curIndex, find all windows k such that:
      //   k * slide <= curIndex < k * slide + size, and k >= 0
      // The first valid k: max(0, ceil((curIndex - size + 1) / slide))
      // The last valid k: floor(curIndex / slide)
      long firstWindow = curIndex < size ? 0 : ceilDiv(curIndex - size + 1, slide);
      long lastWindow = curIndex / slide;
      if (firstWindow <= lastWindow) {
        for (long k = firstWindow; ; k++) {
          // Verify: k * slide <= curIndex < k * slide + size
          if (isIndexInWindow(curIndex, k, slide, size)) {
            properColumnBuilders.get(0).writeLong(k);
            passThroughIndexBuilder.writeLong(curIndex);
          }
          if (k == lastWindow) {
            break;
          }
        }
      }
      curIndex++;
    }

    private static long ceilDiv(long dividend, long divisor) {
      return dividend == 0 ? 0 : (dividend - 1) / divisor + 1;
    }

    private static boolean isIndexInWindow(long curIndex, long windowIndex, long slide, long size) {
      if (windowIndex > Long.MAX_VALUE / slide) {
        return false;
      }
      long windowStart = windowIndex * slide;
      return windowStart <= curIndex && curIndex - windowStart < size;
    }
  }
}
