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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.Set;

public class ValueFill extends IFill implements Cloneable {

  private String value;

  private TsPrimitiveType tsPrimitiveType;

  private String singleStringValue;

  public ValueFill(String singleStringValue) {
    this.singleStringValue = singleStringValue;
  }

  public ValueFill(String value, TSDataType dataType) {
    this.value = value;
    this.dataType = dataType;
    parseTsPrimitiveType();
  }

  @Override
  public IFill copy() {
    return (IFill) clone();
  }

  @Override
  public Object clone() {
    ValueFill valueFill = null;
    try {
      valueFill = (ValueFill) super.clone();
    } catch (CloneNotSupportedException ignored) {
    }
    return valueFill;
  }

  @Override
  public void configureFill(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      Set<String> deviceMeasurements,
      QueryContext context) {
    this.queryStartTime = queryTime;
  }

  @Override
  public TimeValuePair getFillResult() {
    if (tsPrimitiveType != null) {
      switch (dataType) {
        case BOOLEAN:
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case TEXT:
          return new TimeValuePair(queryStartTime, tsPrimitiveType);
        default:
          throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
      }
    } else {
      return null;
    }
  }

  public TimeValuePair getSpecifiedFillResult(TSDataType dataType) throws QueryProcessException {
    switch (dataType) {
      case BOOLEAN:
        // Fill only if the fill value is true or false
        if (singleStringValue.equals("true")) {
          return new TimeValuePair(queryStartTime, new TsPrimitiveType.TsBoolean(true));
        } else if (singleStringValue.equals("false")) {
          return new TimeValuePair(queryStartTime, new TsPrimitiveType.TsBoolean(false));
        } else {
          return null;
        }
      case INT32:
        return new TimeValuePair(
            queryStartTime, new TsPrimitiveType.TsInt(Integer.parseInt(singleStringValue)));
      case INT64:
        return new TimeValuePair(
            queryStartTime, new TsPrimitiveType.TsLong(Long.parseLong(singleStringValue)));
      case FLOAT:
        return new TimeValuePair(
            queryStartTime, new TsPrimitiveType.TsFloat(Float.parseFloat(singleStringValue)));
      case DOUBLE:
        return new TimeValuePair(
            queryStartTime, new TsPrimitiveType.TsDouble(Double.parseDouble(singleStringValue)));
      case TEXT:
        return new TimeValuePair(
            queryStartTime, new TsPrimitiveType.TsBinary(Binary.valueOf(singleStringValue)));
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }

  @Override
  void constructFilter() {}

  private void parseTsPrimitiveType() {
    switch (dataType) {
      case BOOLEAN:
        this.tsPrimitiveType = new TsPrimitiveType.TsBoolean(Boolean.parseBoolean(value));
        break;
      case INT32:
        this.tsPrimitiveType = new TsPrimitiveType.TsInt(Integer.parseInt(value));
        break;
      case INT64:
        this.tsPrimitiveType = new TsPrimitiveType.TsLong(Long.parseLong(value));
        break;
      case FLOAT:
        this.tsPrimitiveType = new TsPrimitiveType.TsFloat(Float.parseFloat(value));
        break;
      case DOUBLE:
        this.tsPrimitiveType = new TsPrimitiveType.TsDouble(Double.parseDouble(value));
        break;
      case TEXT:
        this.tsPrimitiveType = new TsPrimitiveType.TsBinary(Binary.valueOf(value));
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }
}
