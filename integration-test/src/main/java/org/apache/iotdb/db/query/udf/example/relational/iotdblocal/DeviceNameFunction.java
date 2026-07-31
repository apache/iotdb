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
import org.apache.iotdb.udf.api.customizer.analysis.ScalarFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

import java.util.HashMap;
import java.util.Map;

/** Lookup device_name by device_id via a single IoTDBLocal query in {@link #beforeStart}. */
public class DeviceNameFunction implements ScalarFunction {

  private Map<String, String> idToName = Map.of();

  @Override
  public ScalarFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    return new ScalarFunctionAnalysis.Builder().outputDataType(Type.STRING).build();
  }

  @Override
  public void beforeStart(FunctionArguments arguments, IoTDBLocal local) throws UDFException {
    local.info("DeviceNameFunction: loading device_info");
    Map<String, String> map = new HashMap<>();
    try (UDFResultSet rs = local.query("SELECT device_id, device_name FROM device_info")) {
      while (rs.hasNext()) {
        Record row = rs.next();
        map.put(row.getString(0), row.getString(1));
      }
    }
    idToName = map;
    local.info("DeviceNameFunction: loaded {} mappings", idToName.size());
  }

  @Override
  public Object evaluate(Record input) {
    return lookupName(input);
  }

  @Override
  public Object evaluate(Record input, IoTDBLocal local) {
    return lookupName(input);
  }

  private Object lookupName(Record input) {
    String deviceId = input.getString(0);
    return new Binary(idToName.getOrDefault(deviceId, "未知设备"), TSFileConfig.STRING_CHARSET);
  }
}
