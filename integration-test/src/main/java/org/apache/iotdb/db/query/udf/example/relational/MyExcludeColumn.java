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

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MyExcludeColumn implements TableFunction {
  private final String TBL_PARAM = "DATA";
  private final String COL_PARAM = "EXCLUDE";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(TBL_PARAM).rowSemantics().build(),
        ScalarParameterSpecification.builder().name(COL_PARAM).type(Type.STRING).build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(TBL_PARAM);
    String excludeColumn = (String) ((ScalarArgument) arguments.get(COL_PARAM)).getValue();
    List<Integer> requiredColumns = new ArrayList<>();
    DescribedSchema.Builder schemaBuilder = DescribedSchema.builder();
    for (int i = 0; i < tableArgument.getFieldNames().size(); i++) {
      Optional<String> fieldName = tableArgument.getFieldNames().get(i);
      if (!fieldName.isPresent() || !fieldName.get().equalsIgnoreCase(excludeColumn)) {
        requiredColumns.add(i);
        schemaBuilder.addField(fieldName, tableArgument.getFieldTypes().get(i));
      }
    }
    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .requiredColumns(TBL_PARAM, requiredColumns)
        .handle(new MapTableFunctionHandle())
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
        return (input, properColumnBuilders, passThroughIndexBuilder) -> {
          for (int i = 0; i < input.size(); i++) {
            if (input.isNull(i)) {
              properColumnBuilders.get(i).appendNull();
            } else {
              properColumnBuilders.get(i).writeObject(input.getObject(i));
            }
          }
        };
      }
    };
  }
}
