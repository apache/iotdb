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
import org.apache.iotdb.udf.api.UDFResultSet;
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
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Leaf table function: loads device_id via {@link IoTDBLocal#query(String)} in {@link
 * TableFunctionLeafProcessor#beforeStart(IoTDBLocal)}.
 */
public class DeviceIdListTableFunction implements TableFunction {

  private static final String PREFIX_PARAMETER_NAME = "prefix";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Collections.singletonList(
        ScalarParameterSpecification.builder()
            .name(PREFIX_PARAMETER_NAME)
            .type(Type.STRING)
            .defaultValue("")
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    DescribedSchema schema = DescribedSchema.builder().addField("device_id", Type.STRING).build();
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(
                PREFIX_PARAMETER_NAME,
                ((ScalarArgument) arguments.get(PREFIX_PARAMETER_NAME)).getValue())
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
        return new BeforeStartProcessor(
            (String)
                ((MapTableFunctionHandle) tableFunctionHandle).getProperty(PREFIX_PARAMETER_NAME));
      }
    };
  }

  private static class BeforeStartProcessor implements TableFunctionLeafProcessor {
    private final String prefix;
    private final List<String> deviceIds = new ArrayList<>();
    private boolean finish;

    BeforeStartProcessor(String prefix) {
      this.prefix = prefix == null ? "" : prefix;
    }

    @Override
    public void beforeStart(IoTDBLocal local) throws UDFException {
      try (UDFResultSet rs = local.query("SELECT device_id FROM device_info ORDER BY device_id")) {
        while (rs.hasNext()) {
          Record row = rs.next();
          deviceIds.add(row.getString(0));
        }
      }
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      writeDeviceIds(columnBuilders);
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders, IoTDBLocal local) {
      writeDeviceIds(columnBuilders);
    }

    private void writeDeviceIds(List<ColumnBuilder> columnBuilders) {
      if (finish) {
        return;
      }
      for (String deviceId : deviceIds) {
        columnBuilders
            .get(0)
            .writeBinary(new Binary(prefix + deviceId, TSFileConfig.STRING_CHARSET));
      }
      finish = true;
    }

    @Override
    public boolean isFinish() {
      return finish;
    }
  }
}
