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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.exception.UDFOutputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;
import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;
import java.util.HashMap;

public class UDTFJexl implements UDTF {

  private int inputSeriesNumber;
  private TSDataType[] inputDataType;
  private TSDataType outputDataType;
  private JexlScript script;
  private Evaluator evaluator;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    inputSeriesNumber = validator.getParameters().getChildExpressionsSize();
    for (int i = 0; i < inputSeriesNumber; i++) {
      validator.validateInputSeriesDataType(
          i, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE, Type.TEXT, Type.BOOLEAN);
    }
    validator.validateRequiredAttribute("expr");
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws UDFInputSeriesDataTypeNotValidException,
          UDFOutputSeriesDataTypeNotValidException,
          MetadataException {
    String expr = parameters.getString("expr");
    JexlEngine jexl = new JexlBuilder().create();
    script = jexl.createScript(expr);

    inputDataType = new TSDataType[inputSeriesNumber];
    for (int i = 0; i < inputSeriesNumber; i++) {
      inputDataType[i] = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(i));
    }
    outputDataType = probeOutputDataType();

    if (inputSeriesNumber == 1) {
      switch (inputDataType[0]) {
        case INT32:
          evaluator = new EvaluatorIntInput();
          break;
        case INT64:
          evaluator = new EvaluatorLongInput();
          break;
        case FLOAT:
          evaluator = new EvaluatorFloatInput();
          break;
        case DOUBLE:
          evaluator = new EvaluatorDoubleInput();
          break;
        case TEXT:
          evaluator = new EvaluatorStringInput();
          break;
        case BOOLEAN:
          evaluator = new EvaluatorBooleanInput();
          break;
        case STRING:
        case TIMESTAMP:
        case DATE:
        case BLOB:
        default:
          throw new UDFInputSeriesDataTypeNotValidException(
              0,
              UDFDataTypeTransformer.transformToUDFDataType(inputDataType[0]),
              Type.INT32,
              Type.INT64,
              Type.FLOAT,
              Type.DOUBLE,
              Type.TEXT,
              Type.BOOLEAN);
      }
    } else {
      evaluator = new EvaluatorMulInput();
    }

    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(outputDataType));
  }

  // 23, 23L, 23f, 23d, "string", true are hard codes for probing
  private HashMap<TSDataType, Object> initialMap() {
    HashMap<TSDataType, Object> map = new HashMap<>();
    map.put(TSDataType.INT32, 23);
    map.put(TSDataType.INT64, 23L);
    map.put(TSDataType.FLOAT, 23f);
    map.put(TSDataType.DOUBLE, 23d);
    map.put(TSDataType.TEXT, "string");
    map.put(TSDataType.BOOLEAN, true);
    return map;
  }

  private TSDataType probeOutputDataType() throws UDFOutputSeriesDataTypeNotValidException {
    // initial inputHardCodes to probe OutputDataType
    HashMap<TSDataType, Object> map = initialMap();
    Object[] inputHardCodes = new Object[inputSeriesNumber];
    for (int i = 0; i < inputSeriesNumber; i++) {
      inputHardCodes[i] = map.get(inputDataType[i]);
    }

    Object o = script.execute(null, inputHardCodes);

    if (o instanceof Number) {
      return TSDataType.DOUBLE;
    } else if (o instanceof String) {
      return TSDataType.TEXT;
    } else if (o instanceof Boolean) {
      return TSDataType.BOOLEAN;
    } else {
      throw new UDFOutputSeriesDataTypeNotValidException(0, "[Number, String, Boolean]");
    }
  }

  @Override
  public void transform(Row row, PointCollector collector)
      throws IOException,
          UDFOutputSeriesDataTypeNotValidException,
          UDFInputSeriesDataTypeNotValidException {
    switch (outputDataType) {
      case DOUBLE:
        evaluator.evaluateDouble(row, collector);
        break;
      case TEXT:
        evaluator.evaluateText(row, collector);
        break;
      case BOOLEAN:
        evaluator.evaluateBoolean(row, collector);
        break;
      case TIMESTAMP:
      case DATE:
      case STRING:
      case BLOB:
      case INT64:
      case INT32:
      case FLOAT:
      default:
        // This will not happen.
        throw new UDFOutputSeriesDataTypeNotValidException(0, "[Number, String, Boolean]");
    }
  }

  private interface Evaluator {
    void evaluateDouble(Row row, PointCollector collector)
        throws IOException, UDFInputSeriesDataTypeNotValidException;

    void evaluateText(Row row, PointCollector collector)
        throws IOException, UDFInputSeriesDataTypeNotValidException;

    void evaluateBoolean(Row row, PointCollector collector)
        throws IOException, UDFInputSeriesDataTypeNotValidException;
  }

  private class EvaluatorIntInput implements Evaluator {
    @Override
    public void evaluateDouble(Row row, PointCollector collector) throws IOException {
      collector.putDouble(
          row.getTime(), ((Number) script.execute(null, row.getInt(0))).doubleValue());
    }

    @Override
    public void evaluateText(Row row, PointCollector collector) throws IOException {
      collector.putString(row.getTime(), (String) script.execute(null, row.getInt(0)));
    }

    @Override
    public void evaluateBoolean(Row row, PointCollector collector) throws IOException {
      collector.putBoolean(row.getTime(), (Boolean) script.execute(null, row.getInt(0)));
    }
  }

  private class EvaluatorLongInput implements Evaluator {
    @Override
    public void evaluateDouble(Row row, PointCollector collector) throws IOException {
      collector.putDouble(
          row.getTime(), ((Number) script.execute(null, row.getLong(0))).doubleValue());
    }

    @Override
    public void evaluateText(Row row, PointCollector collector) throws IOException {
      collector.putString(row.getTime(), (String) script.execute(null, row.getLong(0)));
    }

    @Override
    public void evaluateBoolean(Row row, PointCollector collector) throws IOException {
      collector.putBoolean(row.getTime(), (Boolean) script.execute(null, row.getLong(0)));
    }
  }

  private class EvaluatorFloatInput implements Evaluator {
    @Override
    public void evaluateDouble(Row row, PointCollector collector) throws IOException {
      collector.putDouble(
          row.getTime(), ((Number) script.execute(null, row.getFloat(0))).doubleValue());
    }

    @Override
    public void evaluateText(Row row, PointCollector collector) throws IOException {
      collector.putString(row.getTime(), (String) script.execute(null, row.getFloat(0)));
    }

    @Override
    public void evaluateBoolean(Row row, PointCollector collector) throws IOException {
      collector.putBoolean(row.getTime(), (Boolean) script.execute(null, row.getFloat(0)));
    }
  }

  private class EvaluatorDoubleInput implements Evaluator {
    @Override
    public void evaluateDouble(Row row, PointCollector collector) throws IOException {
      collector.putDouble(
          row.getTime(), ((Number) script.execute(null, row.getDouble(0))).doubleValue());
    }

    @Override
    public void evaluateText(Row row, PointCollector collector) throws IOException {
      collector.putString(row.getTime(), (String) script.execute(null, row.getDouble(0)));
    }

    @Override
    public void evaluateBoolean(Row row, PointCollector collector) throws IOException {
      collector.putBoolean(row.getTime(), (Boolean) script.execute(null, row.getDouble(0)));
    }
  }

  private class EvaluatorStringInput implements Evaluator {
    @Override
    public void evaluateDouble(Row row, PointCollector collector) throws IOException {
      collector.putDouble(
          row.getTime(), ((Number) script.execute(null, row.getString(0))).doubleValue());
    }

    @Override
    public void evaluateText(Row row, PointCollector collector) throws IOException {
      collector.putString(row.getTime(), (String) script.execute(null, row.getString(0)));
    }

    @Override
    public void evaluateBoolean(Row row, PointCollector collector) throws IOException {
      collector.putBoolean(row.getTime(), (Boolean) script.execute(null, row.getString(0)));
    }
  }

  private class EvaluatorBooleanInput implements Evaluator {
    @Override
    public void evaluateDouble(Row row, PointCollector collector) throws IOException {
      collector.putDouble(
          row.getTime(), ((Number) script.execute(null, row.getBoolean(0))).doubleValue());
    }

    @Override
    public void evaluateText(Row row, PointCollector collector) throws IOException {
      collector.putString(row.getTime(), (String) script.execute(null, row.getBoolean(0)));
    }

    @Override
    public void evaluateBoolean(Row row, PointCollector collector) throws IOException {
      collector.putBoolean(row.getTime(), (Boolean) script.execute(null, row.getBoolean(0)));
    }
  }

  private class EvaluatorMulInput implements Evaluator {

    Object[] values = new Object[inputSeriesNumber];

    @Override
    public void evaluateDouble(Row row, PointCollector collector)
        throws IOException, UDFInputSeriesDataTypeNotValidException {
      getValues(row);
      collector.putDouble(row.getTime(), ((Number) script.execute(null, values)).doubleValue());
    }

    @Override
    public void evaluateText(Row row, PointCollector collector)
        throws IOException, UDFInputSeriesDataTypeNotValidException {
      getValues(row);
      collector.putString(row.getTime(), (String) script.execute(null, values));
    }

    @Override
    public void evaluateBoolean(Row row, PointCollector collector)
        throws IOException, UDFInputSeriesDataTypeNotValidException {
      getValues(row);
      collector.putBoolean(row.getTime(), (Boolean) script.execute(null, values));
    }

    public void getValues(Row row) throws IOException, UDFInputSeriesDataTypeNotValidException {
      for (int i = 0; i < inputSeriesNumber; i++) {
        switch (inputDataType[i]) {
          case INT32:
            values[i] = row.getInt(i);
            break;
          case INT64:
            values[i] = row.getLong(i);
            break;
          case FLOAT:
            values[i] = row.getFloat(i);
            break;
          case DOUBLE:
            values[i] = row.getDouble(i);
            break;
          case TEXT:
            values[i] = row.getString(i);
            break;
          case BOOLEAN:
            values[i] = row.getBoolean(i);
            break;
          case STRING:
          case BLOB:
          case DATE:
          case TIMESTAMP:
          default:
            throw new UDFInputSeriesDataTypeNotValidException(
                i,
                UDFDataTypeTransformer.transformToUDFDataType(inputDataType[i]),
                Type.INT32,
                Type.INT64,
                Type.FLOAT,
                Type.DOUBLE,
                Type.TEXT,
                Type.BOOLEAN);
        }
      }
    }
  }
}
