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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public abstract class UDTFSelectK implements UDTF {

  protected int k;
  protected TSDataType dataType;
  protected PriorityQueue<Pair<Long, Integer>> intPQ;
  protected PriorityQueue<Pair<Long, Long>> longPQ;
  protected PriorityQueue<Pair<Long, Float>> floatPQ;
  protected PriorityQueue<Pair<Long, Double>> doublePQ;
  protected PriorityQueue<Pair<Long, String>> stringPQ;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE, Type.TEXT)
        .validateRequiredAttribute("k")
        .validate(
            k -> 0 < (int) k && (int) k <= 1000,
            "k has to be greater than 0 and less than or equal to 1000.",
            validator.getParameters().getInt("k"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException, UDFInputSeriesDataTypeNotValidException {
    k = parameters.getInt("k");
    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    constructPQ();
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));
  }

  protected abstract void constructPQ() throws UDFInputSeriesDataTypeNotValidException;

  @Override
  public void transform(Row row, PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException {
    switch (dataType) {
      case INT32:
        transformInt(row.getTime(), row.getInt(0));
        break;
      case INT64:
        transformLong(row.getTime(), row.getLong(0));
        break;
      case FLOAT:
        transformFloat(row.getTime(), row.getFloat(0));
        break;
      case DOUBLE:
        transformDouble(row.getTime(), row.getDouble(0));
        break;
      case TEXT:
        transformString(row.getTime(), row.getString(0));
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE,
            Type.TEXT);
    }
  }

  protected abstract void transformInt(long time, int value);

  protected abstract void transformLong(long time, long value);

  protected abstract void transformFloat(long time, float value);

  protected abstract void transformDouble(long time, double value);

  protected abstract void transformString(long time, String value);

  @Override
  public void terminate(PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException {
    switch (dataType) {
      case INT32:
        for (Pair<Long, Integer> pair :
            intPQ.stream().sorted(Comparator.comparing(p -> p.left)).collect(Collectors.toList())) {
          collector.putInt(pair.left, pair.right);
        }
        break;
      case INT64:
        for (Pair<Long, Long> pair :
            longPQ.stream()
                .sorted(Comparator.comparing(p -> p.left))
                .collect(Collectors.toList())) {
          collector.putLong(pair.left, pair.right);
        }
        break;
      case FLOAT:
        for (Pair<Long, Float> pair :
            floatPQ.stream()
                .sorted(Comparator.comparing(p -> p.left))
                .collect(Collectors.toList())) {
          collector.putFloat(pair.left, pair.right);
        }
        break;
      case DOUBLE:
        for (Pair<Long, Double> pair :
            doublePQ.stream()
                .sorted(Comparator.comparing(p -> p.left))
                .collect(Collectors.toList())) {
          collector.putDouble(pair.left, pair.right);
        }
        break;
      case TEXT:
        for (Pair<Long, String> pair :
            stringPQ.stream()
                .sorted(Comparator.comparing(p -> p.left))
                .collect(Collectors.toList())) {
          collector.putString(pair.left, pair.right);
        }
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE,
            Type.TEXT);
    }
  }
}
