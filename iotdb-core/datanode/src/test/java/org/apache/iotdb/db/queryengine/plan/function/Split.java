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

package org.apache.iotdb.db.queryengine.plan.function;

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Binary;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Split implements TableFunction {
  private final String INPUT_PARAMETER_NAME = "INPUT";
  private final String SPLIT_PARAMETER_NAME = "SPLIT";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        ScalarParameterSpecification.builder().name(INPUT_PARAMETER_NAME).type(Type.STRING).build(),
        ScalarParameterSpecification.builder()
            .name(SPLIT_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue(",")
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    DescribedSchema schema = DescribedSchema.builder().addField("output", Type.STRING).build();
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(
                INPUT_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(INPUT_PARAMETER_NAME)).getValue())
            .addProperty(
                SPLIT_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(SPLIT_PARAMETER_NAME)).getValue())
            .build();
    return TableFunctionAnalysis.builder().properColumnSchema(schema).handle(handle).build();
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
      public TableFunctionLeafProcessor getSplitProcessor() {
        return new SplitProcessor(
            (String)
                ((MapTableFunctionHandle) tableFunctionHandle).getProperty(INPUT_PARAMETER_NAME),
            (String)
                ((MapTableFunctionHandle) tableFunctionHandle).getProperty(SPLIT_PARAMETER_NAME));
      }
    };
  }

  private static class SplitProcessor implements TableFunctionLeafProcessor {
    private final String input;
    private final String split;
    private boolean finish = false;

    SplitProcessor(String input, String split) {
      this.input = input;
      this.split = split;
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      for (String s : input.split(split)) {
        columnBuilders.get(0).writeBinary(new Binary(s, Charset.defaultCharset()));
      }
      finish = true;
    }

    @Override
    public boolean isFinish() {
      return finish;
    }
  }
}
