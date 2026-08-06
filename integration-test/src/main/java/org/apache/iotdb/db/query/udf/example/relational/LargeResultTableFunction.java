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
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LargeResultTableFunction implements TableFunction {

  private static final String TABLE_PARAMETER_NAME = "DATA";
  private static final String REPEAT_COUNT_PARAMETER_NAME = "REPEAT_COUNT";
  private static final String PAYLOAD_SIZE_PARAMETER_NAME = "PAYLOAD_SIZE";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder()
            .name(TABLE_PARAMETER_NAME)
            .rowSemantics()
            .passThroughColumns()
            .build(),
        ScalarParameterSpecification.builder()
            .name(REPEAT_COUNT_PARAMETER_NAME)
            .type(Type.INT32)
            .build(),
        ScalarParameterSpecification.builder()
            .name(PAYLOAD_SIZE_PARAMETER_NAME)
            .type(Type.INT32)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(
                REPEAT_COUNT_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(REPEAT_COUNT_PARAMETER_NAME)).getValue())
            .addProperty(
                PAYLOAD_SIZE_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(PAYLOAD_SIZE_PARAMETER_NAME)).getValue())
            .build();
    return TableFunctionAnalysis.builder()
        .properColumnSchema(
            DescribedSchema.builder()
                .addField("repeat_index", Type.INT32)
                .addField("payload", Type.STRING)
                .build())
        .requiredColumns(TABLE_PARAMETER_NAME, Collections.singletonList(0))
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
          private final int repeatCount =
              (int)
                  ((MapTableFunctionHandle) tableFunctionHandle)
                      .getProperty(REPEAT_COUNT_PARAMETER_NAME);
          private final String payloadSuffix =
              "x"
                  .repeat(
                      (int)
                          ((MapTableFunctionHandle) tableFunctionHandle)
                              .getProperty(PAYLOAD_SIZE_PARAMETER_NAME));
          private long recordIndex;

          @Override
          public void process(
              Record input,
              List<ColumnBuilder> properColumnBuilders,
              ColumnBuilder passThroughIndexBuilder) {
            for (int repeatIndex = 0; repeatIndex < repeatCount; repeatIndex++) {
              properColumnBuilders.get(0).writeInt(repeatIndex);
              properColumnBuilders
                  .get(1)
                  .writeBinary(
                      new Binary(repeatIndex + ":" + payloadSuffix, TSFileConfig.STRING_CHARSET));
              passThroughIndexBuilder.writeLong(recordIndex);
            }
            recordIndex++;
          }
        };
      }
    };
  }
}
