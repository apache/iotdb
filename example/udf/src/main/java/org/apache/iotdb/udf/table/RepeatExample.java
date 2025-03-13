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

package org.apache.iotdb.udf.table;

import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
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

/**
 * This is an internal example of the TableFunction implementation. This function is declared as set
 * semantic with pass through columns.
 *
 * <p>CREATE DATABASE test;
 *
 * <p>USE test;
 *
 * <p>CREATE TABLE t1(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD);
 *
 * <p>INSERT INTO t1(time, device_id, s1, s2) VALUES (1, 'd1', 'a', 1), (2, 'd1', null, 2), (3,
 * 'd1', 'c', null);
 *
 * <p>CREATE FUNCTION repeat AS 'org.apache.iotdb.udf.table.RepeatExample';
 *
 * <p>SHOW FUNCTIONS;
 *
 * <p>SELECT * FROM TABLE(repeat(TABLE(t1), 2));
 */
public class RepeatExample implements TableFunction {
  private final String TBL_PARAM = "DATA";
  private final String N_PARAM = "N";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        TableParameterSpecification.builder().name(TBL_PARAM).passThroughColumns().build(),
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
    return TableFunctionAnalysis.builder()
        .properColumnSchema(DescribedSchema.builder().addField("repeat_index", Type.INT32).build())
        .requiredColumns(
            TBL_PARAM,
            Collections.singletonList(0)) // per spec, function must require at least one column
        .build();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(Map<String, Argument> arguments) {
    ScalarArgument count = (ScalarArgument) arguments.get("N");
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new TableFunctionDataProcessor() {
          private final int n = (int) count.getValue();
          private long recordIndex = 0;

          @Override
          public void process(
              Record input,
              List<ColumnBuilder> properColumnBuilders,
              ColumnBuilder passThroughIndexBuilder) {
            properColumnBuilders.get(0).writeInt(0);
            passThroughIndexBuilder.writeLong(recordIndex++);
          }

          @Override
          public void finish(
              List<ColumnBuilder> columnBuilders, ColumnBuilder passThroughIndexBuilder) {
            for (int i = 1; i < n; i++) {
              for (int j = 0; j < recordIndex; j++) {
                columnBuilders.get(0).writeInt(i);
                passThroughIndexBuilder.writeLong(j);
              }
            }
          }
        };
      }
    };
  }
}
