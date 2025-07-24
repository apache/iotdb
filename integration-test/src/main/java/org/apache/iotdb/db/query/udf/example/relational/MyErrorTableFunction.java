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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** It is a table function with wrong implementation, which is only used for testing. */
public class MyErrorTableFunction implements TableFunction {

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
    // there are different error type.
    ScalarArgument n = (ScalarArgument) arguments.get("N");
    int nValue = (int) n.getValue();
    if (nValue == 0) {
      // do not set required columns
      return TableFunctionAnalysis.builder()
          .properColumnSchema(
              DescribedSchema.builder().addField("proper_column", Type.INT32).build())
          .handle(new MapTableFunctionHandle())
          .build();
    } else if (nValue == 1) {
      // set empty required columns
      return TableFunctionAnalysis.builder()
          .properColumnSchema(
              DescribedSchema.builder().addField("proper_column", Type.INT32).build())
          .requiredColumns(TBL_PARAM, Collections.emptyList())
          .handle(new MapTableFunctionHandle())
          .build();
    } else if (nValue == 2) {
      // set negative required columns
      return TableFunctionAnalysis.builder()
          .properColumnSchema(
              DescribedSchema.builder().addField("proper_column", Type.INT32).build())
          .requiredColumns(TBL_PARAM, Collections.singletonList(-1))
          .handle(new MapTableFunctionHandle())
          .build();
    } else if (nValue == 3) {
      // set required columns out of bound (0~10)
      return TableFunctionAnalysis.builder()
          .properColumnSchema(
              DescribedSchema.builder().addField("proper_column", Type.INT32).build())
          .requiredColumns(TBL_PARAM, IntStream.range(0, 11).boxed().collect(Collectors.toList()))
          .handle(new MapTableFunctionHandle())
          .build();
    } else if (nValue == 4) {
      // specify required columns to unknown table
      return TableFunctionAnalysis.builder()
          .properColumnSchema(
              DescribedSchema.builder().addField("proper_column", Type.INT32).build())
          .requiredColumns("TIMECHO", Collections.singletonList(1))
          .handle(new MapTableFunctionHandle())
          .build();
    }
    throw new UDFArgumentNotValidException("unexpected argument value");
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
        return (input, properColumnBuilders, passThroughIndexBuilder) -> {
          // do nothing
        };
      }
    };
  }
}
