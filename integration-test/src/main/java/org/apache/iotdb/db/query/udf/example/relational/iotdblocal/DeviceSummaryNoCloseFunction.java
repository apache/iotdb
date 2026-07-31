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

/**
 * Same as {@link DeviceSummaryFunction} but intentionally does not close result sets, for verifying
 * framework auto-cleanup.
 */
public class DeviceSummaryNoCloseFunction implements ScalarFunction {

  private Map<String, String> idToName = Map.of();
  private Map<String, Double> idToMaxTemp = Map.of();

  @Override
  public ScalarFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    return new ScalarFunctionAnalysis.Builder().outputDataType(Type.STRING).build();
  }

  @Override
  public void beforeStart(FunctionArguments arguments, IoTDBLocal local) throws UDFException {
    idToName = new HashMap<>();
    idToMaxTemp = new HashMap<>();
    UDFResultSet nameResult = local.query("SELECT device_id, device_name FROM device_info");
    UDFResultSet limitResult = local.query("SELECT device_id, max_temp FROM device_limits");
    boolean hasName = nameResult.hasNext();
    boolean hasLimit = limitResult.hasNext();
    while (hasName || hasLimit) {
      if (hasName) {
        Record row = nameResult.next();
        idToName.put(row.getString(0), row.getString(1));
        hasName = nameResult.hasNext();
      }
      if (hasLimit) {
        Record row = limitResult.next();
        idToMaxTemp.put(row.getString(0), row.getDouble(1));
        hasLimit = limitResult.hasNext();
      }
    }
  }

  @Override
  public Object evaluate(Record input) {
    return buildSummary(input);
  }

  @Override
  public Object evaluate(Record input, IoTDBLocal local) {
    return buildSummary(input);
  }

  private Object buildSummary(Record input) {
    String deviceId = input.getString(0);
    String name = idToName.getOrDefault(deviceId, "未知设备");
    Double maxTemp = idToMaxTemp.get(deviceId);
    return new Binary(
        String.format("%s(上限:%s)", name, maxTemp == null ? "未知" : maxTemp),
        TSFileConfig.STRING_CHARSET);
  }
}
