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

package org.apache.iotdb.db.query.udf.example.relational.iotdblocal;

import org.apache.iotdb.udf.api.IoTDBLocal;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Exercises IoTDBLocal log APIs at each table-function lifecycle hook. */
public class IoTDBLocalLogTableFunction implements TableFunction {

  private static final String INPUT_PARAMETER_NAME = "input";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Collections.singletonList(
        ScalarParameterSpecification.builder()
            .name(INPUT_PARAMETER_NAME)
            .type(Type.STRING)
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
        return new LogSplitProcessor(
            (String)
                ((MapTableFunctionHandle) tableFunctionHandle).getProperty(INPUT_PARAMETER_NAME));
      }
    };
  }

  private static class LogSplitProcessor implements TableFunctionLeafProcessor {
    private final String input;
    private boolean finish;
    private boolean processLogged;
    private boolean destroyLogged;

    LogSplitProcessor(String input) {
      this.input = input;
    }

    @Override
    public void beforeStart(IoTDBLocal local) {
      IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.TVF_BEFORE_START);
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      for (String value : input.split(",")) {
        columnBuilders.get(0).writeBinary(new Binary(value, Charset.defaultCharset()));
      }
      finish = true;
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders, IoTDBLocal local) {
      if (!processLogged) {
        processLogged = true;
        IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.TVF_PROCESS);
      }
      process(columnBuilders);
    }

    @Override
    public boolean isFinish() {
      return finish;
    }

    @Override
    public void beforeDestroy(IoTDBLocal local) {
      if (!destroyLogged) {
        destroyLogged = true;
        IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.TVF_BEFORE_DESTROY);
      }
    }
  }
}
