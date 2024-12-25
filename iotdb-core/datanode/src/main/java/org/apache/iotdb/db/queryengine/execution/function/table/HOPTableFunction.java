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

package org.apache.iotdb.db.queryengine.execution.function.table;

import org.apache.iotdb.udf.api.relational.table.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.Descriptor;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.DescriptorParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ReturnTypeSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.iotdb.udf.api.relational.table.specification.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;

public class HOPTableFunction extends TableFunction {

  private static final String DATA_PARAMETER_NAME = "DATA";
  private static final String TIMECOL_PARAMETER_NAME = "TIMECOL";
  private static final String SLIDE_PARAMETER_NAME = "SLIDE";
  private static final String SIZE_PARAMETER_NAME = "SIZE";

  @Override
  public List<ParameterSpecification> getArgumentsSpecification() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(DATA_PARAMETER_NAME)
            .passThroughColumns()
            .keepWhenEmpty()
            .build(),
        DescriptorParameterSpecification.builder().name(TIMECOL_PARAMETER_NAME).build(),
        ScalarParameterSpecification.builder()
            .name(SLIDE_PARAMETER_NAME)
            .type(org.apache.iotdb.udf.api.type.Type.INT64)
            .build(),
        ScalarParameterSpecification.builder()
            .name(SIZE_PARAMETER_NAME)
            .type(org.apache.iotdb.udf.api.type.Type.INT64)
            .build());
  }

  @Override
  public ReturnTypeSpecification getReturnTypeSpecification() {
    return GENERIC_TABLE;
  }

  @Override
  public Optional<Descriptor> getReturnProperColumns() {
    return Optional.of(
        Descriptor.descriptor(
            Arrays.asList("window_start", "window_end"),
            Arrays.asList(
                org.apache.iotdb.udf.api.type.Type.TIMESTAMP,
                org.apache.iotdb.udf.api.type.Type.TIMESTAMP)));
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) {
    return TableFunctionAnalysis.builder()
        .requiredColumns(
            DATA_PARAMETER_NAME,
            IntStream.range(0, ((TableArgument) arguments.get(DATA_PARAMETER_NAME)).size())
                .boxed()
                .collect(toImmutableList()))
        .build();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(Map<String, Argument> arguments) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return null;
      }
    };
  }
}
