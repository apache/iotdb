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

package org.apache.iotdb.db.query.udf.builtin;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.db.query.udf.api.exception.UDFOutputSeriesDataTypeNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;

import java.io.IOException;

public class UDTFJexl implements UDTF {

  private TSDataType outputDataType;
  private JexlScript script;
  private Evaluator evaluator;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.BOOLEAN)
        .validateRequiredAttribute("expr");
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws UDFInputSeriesDataTypeNotValidException, UDFOutputSeriesDataTypeNotValidException,
          MetadataException {
    String expr = parameters.getString("expr");
    JexlEngine jexl = new JexlBuilder().create();
    script = jexl.createScript(expr);

    TSDataType inputDataType = parameters.getDataType(0);
    outputDataType = probeOutputDataType(inputDataType);

    switch (inputDataType) {
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
      default:
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            inputDataType,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.BOOLEAN);
    }

    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(outputDataType);
  }

  private TSDataType probeOutputDataType(TSDataType inputDataType)
      throws UDFInputSeriesDataTypeNotValidException, UDFOutputSeriesDataTypeNotValidException {
    Object o;
    // 23, 23L, 23f, 23d, "string", true are hard codes for probing
    switch (inputDataType) {
      case INT32:
        o = script.execute(null, 23);
        break;
      case INT64:
        o = script.execute(null, 23L);
        break;
      case FLOAT:
        o = script.execute(null, 23f);
        break;
      case DOUBLE:
        o = script.execute(null, 23d);
        break;
      case TEXT:
        o = script.execute(null, "string");
        break;
      case BOOLEAN:
        o = script.execute(null, true);
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            inputDataType,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.BOOLEAN);
    }

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
      throws IOException, UDFOutputSeriesDataTypeNotValidException, QueryProcessException {
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
      default:
        // This will not happen.
        throw new UDFOutputSeriesDataTypeNotValidException(0, "[Number, String, Boolean]");
    }
  }

  private interface Evaluator {
    void evaluateDouble(Row row, PointCollector collector) throws IOException;

    void evaluateText(Row row, PointCollector collector) throws IOException, QueryProcessException;

    void evaluateBoolean(Row row, PointCollector collector) throws IOException;
  }

  private class EvaluatorIntInput implements Evaluator {
    @Override
    public void evaluateDouble(Row row, PointCollector collector) throws IOException {
      collector.putDouble(
          row.getTime(), ((Number) script.execute(null, row.getInt(0))).doubleValue());
    }

    @Override
    public void evaluateText(Row row, PointCollector collector)
        throws IOException, QueryProcessException {
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
    public void evaluateText(Row row, PointCollector collector)
        throws IOException, QueryProcessException {
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
    public void evaluateText(Row row, PointCollector collector)
        throws IOException, QueryProcessException {
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
    public void evaluateText(Row row, PointCollector collector)
        throws IOException, QueryProcessException {
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
    public void evaluateText(Row row, PointCollector collector)
        throws IOException, QueryProcessException {
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
    public void evaluateText(Row row, PointCollector collector)
        throws IOException, QueryProcessException {
      collector.putString(row.getTime(), (String) script.execute(null, row.getBoolean(0)));
    }

    @Override
    public void evaluateBoolean(Row row, PointCollector collector) throws IOException {
      collector.putBoolean(row.getTime(), (Boolean) script.execute(null, row.getBoolean(0)));
    }
  }
}
