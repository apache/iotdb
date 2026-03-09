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

package org.apache.iotdb.db.query.udf.example.relational;

import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
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

public class MyRepeatWithoutIndex implements TableFunction {
  private final String TBL_PARAM = "DATA";
  private final String N_PARAM = "N";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(TBL_PARAM)
            .rowSemantics()
            .passThroughColumns()
            .build(),
        ScalarParameterSpecification.builder().name(N_PARAM).type(Type.INT32).build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {

    ScalarArgument count = (ScalarArgument) arguments.get("N");
    if (count == null) {
      throw new UDFArgumentNotValidException("count argument for function repeat() is missing");
    } else if ((int) count.getValue() <= 0) {
      throw new UDFArgumentNotValidException(
          "count argument for function repeat() must be positive");
    }
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder().addProperty(N_PARAM, count.getValue()).build();
    return TableFunctionAnalysis.builder()
        .requiredColumns(
            TBL_PARAM,
            Collections.singletonList(0)) // per spec, function must require at least one column
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
          private final int n =
              (int) ((MapTableFunctionHandle) tableFunctionHandle).getProperty(N_PARAM);
          private long recordIndex = 0;

          @Override
          public void process(
              Record input,
              List<ColumnBuilder> properColumnBuilders,
              ColumnBuilder passThroughIndexBuilder) {
            for (int i = 0; i < n; i++) {
              passThroughIndexBuilder.writeLong(recordIndex);
            }
            recordIndex++;
          }
        };
      }
    };
  }
}
