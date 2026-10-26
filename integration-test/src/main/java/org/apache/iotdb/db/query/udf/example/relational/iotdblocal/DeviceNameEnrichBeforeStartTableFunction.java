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
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.EmptyTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Data table function: loads device_name mapping via {@link IoTDBLocal#query(String)} in {@link
 * TableFunctionDataProcessor#beforeStart(IoTDBLocal)}.
 */
public class DeviceNameEnrichBeforeStartTableFunction implements TableFunction {

  private static final String TABLE_PARAM = "DATA";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Collections.singletonList(
        TableParameterSpecification.builder().name(TABLE_PARAM).rowSemantics().build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    TableArgument tableArgument = (TableArgument) arguments.get(TABLE_PARAM);
    int deviceIdIndex = findDeviceIdIndex(tableArgument);
    if (deviceIdIndex < 0) {
      throw new UDFArgumentNotValidException("device_id column is required in table argument");
    }
    return TableFunctionAnalysis.builder()
        .properColumnSchema(DescribedSchema.builder().addField("device_name", Type.STRING).build())
        .requiredColumns(TABLE_PARAM, Collections.singletonList(deviceIdIndex))
        .handle(new EmptyTableFunctionHandle())
        .build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new EmptyTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new BeforeStartProcessor();
      }
    };
  }

  private static int findDeviceIdIndex(TableArgument tableArgument) {
    for (int i = 0; i < tableArgument.getFieldNames().size(); i++) {
      Optional<String> fieldName = tableArgument.getFieldNames().get(i);
      if (fieldName.isPresent() && "device_id".equalsIgnoreCase(fieldName.get())) {
        return i;
      }
    }
    return -1;
  }

  private static class BeforeStartProcessor implements TableFunctionDataProcessor {
    private Map<String, String> idToName = Map.of();

    @Override
    public void beforeStart(IoTDBLocal local) throws UDFException {
      Map<String, String> map = new HashMap<>();
      try (UDFResultSet rs = local.query("SELECT device_id, device_name FROM device_info")) {
        while (rs.hasNext()) {
          Record row = rs.next();
          map.put(row.getString(0), row.getString(1));
        }
      }
      idToName = map;
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder) {
      writeDeviceName(input, properColumnBuilders);
    }

    @Override
    public void process(
        Record input,
        List<ColumnBuilder> properColumnBuilders,
        ColumnBuilder passThroughIndexBuilder,
        IoTDBLocal local) {
      writeDeviceName(input, properColumnBuilders);
    }

    private void writeDeviceName(Record input, List<ColumnBuilder> properColumnBuilders) {
      String deviceId = input.getString(0);
      properColumnBuilders
          .get(0)
          .writeBinary(
              new Binary(idToName.getOrDefault(deviceId, "未知设备"), TSFileConfig.STRING_CHARSET));
    }
  }
}
