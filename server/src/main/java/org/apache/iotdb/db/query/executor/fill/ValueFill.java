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
package org.apache.iotdb.db.query.executor.fill;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.Set;

public class ValueFill extends IFill {
  private PartialPath seriesPath;
  private QueryContext context;
  private String value;
  private Set<String> allSensors;

  public ValueFill(TSDataType dataType, long queryTime, String value) {
    super(dataType, queryTime);
    this.value = value;
  }

  public ValueFill(String value) {
    this.value = value;
  }

  @Override
  public IFill copy() {
    return new ValueFill(dataType, queryTime, value);
  }

  @Override
  public void configureFill(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      Set<String> deviceMeasurements,
      QueryContext context) {
    this.seriesPath = path;
    this.dataType = dataType;
    this.context = context;
    this.queryTime = queryTime;
    this.allSensors = deviceMeasurements;

  }

  @Override
  public TimeValuePair getFillResult() {
    switch (dataType) {
      case BOOLEAN:
        return new TimeValuePair(
            queryTime, new TsPrimitiveType.TsBoolean(Boolean.parseBoolean(value)));
      case INT32:
        return new TimeValuePair(queryTime, new TsPrimitiveType.TsInt(Integer.parseInt(value)));
      case INT64:
        return new TimeValuePair(queryTime, new TsPrimitiveType.TsLong(Long.parseLong(value)));
      case FLOAT:
        return new TimeValuePair(queryTime, new TsPrimitiveType.TsFloat(Float.parseFloat(value)));
      case DOUBLE:
        return new TimeValuePair(
            queryTime, new TsPrimitiveType.TsDouble(Double.parseDouble(value)));
      case TEXT:
        return new TimeValuePair(queryTime, new TsPrimitiveType.TsBinary(Binary.valueOf(value)));
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }

  @Override
  void constructFilter() {}
}
