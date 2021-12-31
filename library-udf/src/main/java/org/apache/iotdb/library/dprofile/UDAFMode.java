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
package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.dprofile.util.MaxSelector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.eclipse.collections.impl.map.mutable.primitive.DoubleIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.FloatIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;

import java.util.HashMap;
import java.util.Map;

/** This function aggregates mode of input series. */
public class UDAFMode implements UDTF {

  private IntIntHashMap intMap;
  private LongIntHashMap longMap;
  private FloatIntHashMap floatMap;
  private DoubleIntHashMap doubleMap;
  private int booleanCnt;
  private HashMap<String, Integer> stringMap;
  private TSDataType dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateInputSeriesNumber(1);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(parameters.getDataType(0));
    dataType = parameters.getDataType(0);
    switch (dataType) {
      case INT32:
        intMap = new IntIntHashMap();
        break;
      case INT64:
        longMap = new LongIntHashMap();
        break;
      case FLOAT:
        floatMap = new FloatIntHashMap();
        break;
      case DOUBLE:
        doubleMap = new DoubleIntHashMap();
        break;
      case TEXT:
        stringMap = new HashMap<>();
        break;
      case BOOLEAN:
        booleanCnt = 0;
    }
  }

  @Override
  public void transform(Row row, PointCollector pc) throws Exception {
    switch (dataType) {
      case INT32:
        intMap.addToValue(row.getInt(0), 1);
        break;
      case INT64:
        longMap.addToValue(row.getLong(0), 1);
        break;
      case FLOAT:
        floatMap.addToValue(row.getFloat(0), 1);
        break;
      case DOUBLE:
        doubleMap.addToValue(row.getDouble(0), 1);
        break;
      case TEXT:
        stringMap.put(row.getString(0), stringMap.getOrDefault(row.getString(0), 0) + 1);
        break;
      case BOOLEAN:
        boolean v = row.getBoolean(0);
        booleanCnt = v ? booleanCnt + 1 : booleanCnt - 1;
    }
  }

  @Override
  public void terminate(PointCollector pc) throws Exception {
    MaxSelector max = new MaxSelector();
    switch (dataType) {
      case INT32:
        intMap.forEachKeyValue(max::insert);
        pc.putInt(0, max.getInt());
        break;
      case INT64:
        longMap.forEachKeyValue(max::insert);
        pc.putLong(0, max.getLong());
        break;
      case FLOAT:
        floatMap.forEachKeyValue(max::insert);
        pc.putFloat(0, max.getFloat());
        break;
      case DOUBLE:
        doubleMap.forEachKeyValue(max::insert);
        pc.putDouble(0, max.getDouble());
        break;
      case TEXT:
        int maxTimes = 0;
        String s = null;
        for (Map.Entry<String, Integer> entry : stringMap.entrySet()) {
          String key = entry.getKey();
          Integer value = entry.getValue();
          if (value > maxTimes) {
            maxTimes = value;
            s = key;
          }
        }
        pc.putString(0, s);
        break;
      case BOOLEAN:
        pc.putBoolean(0, booleanCnt > 0);
    }
  }
}
