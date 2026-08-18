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
package org.apache.iotdb.library.frequency;

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
import org.jtransforms.fft.DoubleFFT_1D;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableFFT implements TableFunction {

  private static final String TBL_PARAM = "DATA";
  private static final String RESULT_PARAM = "RESULT";
  private static final String COMPRESS_PARAM = "COMPRESS";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(TBL_PARAM).passThroughColumns().build(),
        ScalarParameterSpecification.builder()
            .name(RESULT_PARAM)
            .type(Type.STRING)
            .defaultValue("abs")
            .build(),
        ScalarParameterSpecification.builder()
            .name(COMPRESS_PARAM)
            .type(Type.DOUBLE)
            .defaultValue(1.0)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    String result = (String) ((ScalarArgument) arguments.get(RESULT_PARAM)).getValue();
    double compress = (double) ((ScalarArgument) arguments.get(COMPRESS_PARAM)).getValue();

    if (!result.equalsIgnoreCase("real")
        && !result.equalsIgnoreCase("imag")
        && !result.equalsIgnoreCase("abs")
        && !result.equalsIgnoreCase("angle")) {
      throw new UDFException("result must be one of: real, imag, abs, angle");
    }
    if (compress <= 0 || compress > 1) {
      throw new UDFException("compress must be within (0, 1]");
    }

    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(RESULT_PARAM, result)
            .addProperty(COMPRESS_PARAM, compress)
            .build();

    return TableFunctionAnalysis.builder()
        .properColumnSchema(DescribedSchema.builder().addField("fft_value", Type.DOUBLE).build())
        .requiredColumns(TBL_PARAM, Collections.singletonList(0))
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
        return new TableFunctionDataProcessor() {
          private final String result =
              (String) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(RESULT_PARAM);
          private final List<Double> values = new ArrayList<>();

          @Override
          public void process(
              Record input,
              List<ColumnBuilder> properColumnBuilders,
              ColumnBuilder passThroughIndexBuilder) {
            if (!input.isNull(0)) {
              values.add(input.getDouble(0));
            }
          }

          @Override
          public void finish(
              List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
            int n = values.size();
            if (n == 0) return;

            double[] a = new double[2 * n];
            for (int i = 0; i < n; i++) {
              a[2 * i] = values.get(i);
              a[2 * i + 1] = 0;
            }

            DoubleFFT_1D fft = new DoubleFFT_1D(n);
            fft.complexForward(a);

            // output: freq index as passThroughIndex, value to properColumn
            for (int i = 0; i < n / 2; i++) {
              double val = computeValue(a, i);
              properColumnBuilders.get(0).writeDouble(val);
              passThroughIndexBuilder.writeLong(i);
            }
          }

          private double computeValue(double[] a, int i) {
            return switch (result.toLowerCase()) {
              case "real" -> a[2 * i];
              case "imag" -> a[2 * i + 1];
              case "abs" -> Math.sqrt(a[2 * i] * a[2 * i] + a[2 * i + 1] * a[2 * i + 1]);
              case "angle" -> Math.atan2(a[2 * i + 1], a[2 * i]);
              default -> 0;
            };
          }
        };
      }
    };
  }
}
