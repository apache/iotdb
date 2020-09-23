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

package org.apache.iotdb.db.query.udf.example;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.OneByOneAccessStrategy;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class Adder extends UDTF {

  private float addend;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    System.out.println("Adder#beforeStart");
    addend = parameters.getFloatOrDefault("addend", 0);
    configurations
        .setOutputDataType(TSDataType.FLOAT)
        .setAccessStrategy(new OneByOneAccessStrategy());
  }

  @Override
  public void transform(Row row) throws Exception {
    if (row.isNull(0) || row.isNull(1)) {
      return;
    }
    collector.putFloat(row.getTime(),
        extractFloatValue(row, 0) + extractFloatValue(row, 1) + addend);
  }

  private float extractFloatValue(Row row, int index) {
    float value;
    switch (dataTypes.get(index)) {
      case INT32:
        value = (float) row.getInt(index);
        break;
      case INT64:
        value = (float) row.getLong(index);
        break;
      case FLOAT:
        value = row.getFloat(index);
        break;
      case DOUBLE:
        value = (float) row.getDouble(index);
        break;
      default:
        throw new UnSupportedDataTypeException(dataTypes.get(index).toString());
    }
    return value;
  }

  @Override
  public void beforeDestroy() {
    System.out.println("Adder#beforeDestroy");
  }
}
