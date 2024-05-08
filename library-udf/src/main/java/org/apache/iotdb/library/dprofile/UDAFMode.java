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

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.library.dprofile.util.MaxSelector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;

import org.eclipse.collections.impl.map.mutable.primitive.DoubleIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.DoubleLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.FloatIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.FloatLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.util.HashMap;
import java.util.Map;

/** This function aggregates mode of input series. */
public class UDAFMode implements UDTF {

  private IntIntHashMap intMap;
  private IntLongHashMap itMap;
  private LongIntHashMap longMap;
  private LongLongHashMap ltMap;
  private FloatIntHashMap floatMap;
  private FloatLongHashMap ftMap;
  private DoubleIntHashMap doubleMap;
  private DoubleLongHashMap dtMap;
  private int booleanCnt;
  private HashMap<String, Integer> stringMap;
  private HashMap<String, Long> stMap;
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
    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    switch (dataType) {
      case INT32:
        intMap = new IntIntHashMap();
        itMap = new IntLongHashMap();
        break;
      case INT64:
        longMap = new LongIntHashMap();
        ltMap = new LongLongHashMap();
        break;
      case FLOAT:
        floatMap = new FloatIntHashMap();
        ftMap = new FloatLongHashMap();
        break;
      case DOUBLE:
        doubleMap = new DoubleIntHashMap();
        dtMap = new DoubleLongHashMap();
        break;
      case TEXT:
        stringMap = new HashMap<>();
        stMap = new HashMap<>();
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
        if (!itMap.containsKey(row.getInt(0))) {
          itMap.addToValue(row.getInt(0), row.getTime());
        }
        break;
      case INT64:
        longMap.addToValue(row.getLong(0), 1);
        if (!ltMap.containsKey(row.getLong(0))) {
          ltMap.addToValue(row.getLong(0), row.getTime());
        }
        break;
      case FLOAT:
        floatMap.addToValue(row.getFloat(0), 1);
        if (!ftMap.containsKey(row.getFloat(0))) {
          ftMap.addToValue(row.getFloat(0), row.getTime());
        }
        break;
      case DOUBLE:
        doubleMap.addToValue(row.getDouble(0), 1);
        if (!dtMap.containsKey(row.getDouble(0))) {
          dtMap.addToValue(row.getDouble(0), row.getTime());
        }
        break;
      case TEXT:
        stringMap.put(row.getString(0), stringMap.getOrDefault(row.getString(0), 0) + 1);
        if (!stMap.containsKey(row.getString(0))) {
          stMap.put(row.getString(0), row.getTime());
        }
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
        int im = max.getInt();
        pc.putInt(itMap.get(im), im);
        break;
      case INT64:
        longMap.forEachKeyValue(max::insert);
        long lm = max.getLong();
        pc.putLong(ltMap.get(lm), lm);
        break;
      case FLOAT:
        floatMap.forEachKeyValue(max::insert);
        float fm = max.getFloat();
        pc.putFloat(ftMap.get(fm), fm);
        break;
      case DOUBLE:
        doubleMap.forEachKeyValue(max::insert);
        double dm = max.getDouble();
        pc.putDouble(dtMap.get(dm), dm);
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
        pc.putString(stMap.get(s), s);
        break;
      case BOOLEAN:
        pc.putBoolean(0, booleanCnt > 0);
    }
  }
}
