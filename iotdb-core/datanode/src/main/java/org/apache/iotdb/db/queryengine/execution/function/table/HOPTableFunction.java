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

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.Descriptor;
import org.apache.iotdb.udf.api.relational.table.argument.DescriptorArgument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.DescriptorParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ReturnTypeSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    TableArgument tableArgument = (TableArgument) arguments.get(DATA_PARAMETER_NAME);
    DescriptorArgument descriptorArgument =
        (DescriptorArgument) arguments.get(TIMECOL_PARAMETER_NAME);
    String expectedFieldName =
        descriptorArgument
            .getDescriptor()
            .orElseThrow(() -> new UDFException("The descriptor of the argument is empty"))
            .getFields()
            .get(0)
            .getName();
    int requiredIndex = -1;
    for (int i = 0; i < tableArgument.getFieldTypes().size(); i++) {
      Optional<String> fieldName = tableArgument.getFieldNames().get(i);
      if (fieldName.isPresent() && expectedFieldName.equalsIgnoreCase(fieldName.get())) {
        requiredIndex = i;
        break;
      }
    }
    if (requiredIndex == -1) {
      throw new UDFException("The required field is not found in the input table");
    }
    return TableFunctionAnalysis.builder()
        .requiredColumns(DATA_PARAMETER_NAME, Collections.singletonList(requiredIndex))
        .build();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(Map<String, Argument> arguments) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new HOPDataProcessor(
                (Long) ((ScalarArgument) arguments.get(SLIDE_PARAMETER_NAME)).getValue()*1000,
                (Long) ((ScalarArgument) arguments.get(SLIDE_PARAMETER_NAME)).getValue()*1000
        );
      }
    };
  }

  private static class HOPDataProcessor implements TableFunctionDataProcessor {

    private final long slide;
    private final long size;
    private long startTime = Long.MIN_VALUE;

    public HOPDataProcessor(long slide, long size) {
      this.slide = slide;
      this.size = size;
    }

    @Override
    public void process(Record input, List<ColumnBuilder> columnBuilders) {
      long curTime = input.getLong(0);
      if(startTime==Long.MIN_VALUE) {
        startTime = curTime;
      }
      while (curTime - startTime >= size) {
        startTime += slide;
      }

      for (int i = 0; i < input.size(); i++) {
        if(input.isNull(i)) {
          columnBuilders.get(i + 2).appendNull();
        } else {
          columnBuilders.get(i + 2).writeObject(input.getObject(i));
        }
      }
        columnBuilders.get(0).writeLong(startTime);
        columnBuilders.get(1).writeLong(startTime + size);
    }

    @Override
    public void finish(List<ColumnBuilder> columnBuilders) {}
  }
}
