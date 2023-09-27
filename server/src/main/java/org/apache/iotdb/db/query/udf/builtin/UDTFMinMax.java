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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
import org.apache.iotdb.tsfile.read.common.IOMonitor2.DataSetType;

import java.io.IOException;

public class UDTFMinMax implements UDTF {

  protected TSDataType dataType;
  protected long tqs;
  protected long tqe;
  protected int w;

  private long bottomTime;
  private long topTime;

  private int intMaxV;
  private long longMaxV;
  private float floatMaxV;
  private double doubleMaxV;

  private int intMinV;
  private long longMinV;
  private float floatMinV;
  private double doubleMinV;

  private String[] result;
  private int idx;

  private void init() {
    this.bottomTime = 0;
    this.topTime = 0;

    this.intMinV = Integer.MAX_VALUE;
    this.longMinV = Long.MAX_VALUE;
    this.floatMinV = Float.MAX_VALUE;
    this.doubleMinV = Double.MAX_VALUE;

    this.intMaxV = Integer.MIN_VALUE;
    this.longMaxV = Long.MIN_VALUE;
    this.floatMaxV = Float.MIN_VALUE;
    this.doubleMaxV = Double.MIN_VALUE;
  }

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    IOMonitor2.dataSetType = DataSetType.UDTFAlignByTimeDataSet_M4_POINT;
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validateRequiredAttribute("tqs")
        .validateRequiredAttribute("tqe")
        .validateRequiredAttribute("w");
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    dataType = parameters.getDataType(0);
    tqs = parameters.getLong("tqs"); // closed
    tqe = parameters.getLong("tqe"); // open
    w = parameters.getInt("w");
    if ((tqe - tqs) % w != 0) {
      throw new MetadataException("You should make tqe-tqs integer divide w");
    }
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.TEXT);
    init();
    this.idx = -1;
    result = new String[w];
    for (int i = 0; i < w; i++) {
      result[i] = "empty";
    }
  }

  @Override
  public void transform(Row row, PointCollector collector)
      throws QueryProcessException, IOException {
    if (idx < 0) {
      idx = 0; // means at least not all empty
    }
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
      default:
        break;
    }
  }

  protected void transformInt(long time, int value) throws IOException {
    long intervalLen = (tqe - tqs) / w;
    int pos = (int) Math.floor((time - tqs) * 1.0 / intervalLen);
    if (pos >= w) {
      throw new IOException("Make sure the range time filter is time>=tqs and time<tqe");
    }

    if (pos > idx) {
      result[idx] =
          "BottomPoint=("
              + bottomTime
              + ","
              + intMinV
              + "), "
              + "TopPoint=("
              + topTime
              + ","
              + intMaxV
              + ")";
      idx = pos;
      init(); // clear environment for this new interval
    }
    // update for the current interval
    if (value < intMinV) {
      bottomTime = time;
      intMinV = value;
    }
    if (value > intMaxV) {
      topTime = time;
      intMaxV = value;
    }
  }

  protected void transformLong(long time, long value) throws IOException {
    long intervalLen = (tqe - tqs) / w;
    int pos = (int) Math.floor((time - tqs) * 1.0 / intervalLen);

    if (pos >= w) {
      throw new IOException("Make sure the range time filter is time>=tqs and time<tqe");
    }

    if (pos > idx) {
      result[idx] =
          "BottomPoint=("
              + bottomTime
              + ","
              + longMinV
              + "), "
              + "TopPoint=("
              + topTime
              + ","
              + longMaxV
              + ")";
      idx = pos;
      init(); // clear environment for this new interval
    }

    if (value < longMinV) {
      bottomTime = time;
      longMinV = value;
    }
    if (value > longMaxV) {
      topTime = time;
      longMaxV = value;
    }
  }

  protected void transformFloat(long time, float value) throws IOException {
    long intervalLen = (tqe - tqs) / w;
    int pos = (int) Math.floor((time - tqs) * 1.0 / intervalLen);

    if (pos >= w) {
      throw new IOException("Make sure the range time filter is time>=tqs and time<tqe");
    }

    if (pos > idx) {
      result[idx] =
          "BottomPoint=("
              + bottomTime
              + ","
              + floatMinV
              + "), "
              + "TopPoint=("
              + topTime
              + ","
              + floatMaxV
              + ")";
      idx = pos;
      init(); // clear environment for this new interval
    }

    if (value < floatMinV) {
      bottomTime = time;
      floatMinV = value;
    }
    if (value > floatMaxV) {
      topTime = time;
      floatMaxV = value;
    }
  }

  protected void transformDouble(long time, double value) throws IOException {
    long intervalLen = (tqe - tqs) / w;
    int pos = (int) Math.floor((time - tqs) * 1.0 / intervalLen);

    if (pos >= w) {
      throw new IOException("Make sure the range time filter is time>=tqs and time<tqe");
    }

    if (pos > idx) {
      result[idx] =
          "BottomPoint=("
              + bottomTime
              + ","
              + doubleMinV
              + "), "
              + "TopPoint=("
              + topTime
              + ","
              + doubleMaxV
              + ")";
      idx = pos;
      init(); // clear environment for this new interval
    }

    if (value < doubleMinV) {
      bottomTime = time;
      doubleMinV = value;
    }
    if (value > doubleMaxV) {
      topTime = time;
      doubleMaxV = value;
    }
  }

  @Override
  public void terminate(PointCollector collector) throws IOException, QueryProcessException {
    if (idx >= 0) { // means at least not all empty
      // record the last interval (not necessarily idx=w-1) in the transform stage
      switch (dataType) {
        case INT32:
          result[idx] =
              "BottomPoint=("
                  + bottomTime
                  + ","
                  + intMinV
                  + "), "
                  + "TopPoint=("
                  + topTime
                  + ","
                  + intMaxV
                  + ")";
          break;
        case INT64:
          result[idx] =
              "BottomPoint=("
                  + bottomTime
                  + ","
                  + longMinV
                  + "), "
                  + "TopPoint=("
                  + topTime
                  + ","
                  + longMaxV
                  + ")";
          break;
        case FLOAT:
          result[idx] =
              "BottomPoint=("
                  + bottomTime
                  + ","
                  + floatMinV
                  + "), "
                  + "TopPoint=("
                  + topTime
                  + ","
                  + floatMaxV
                  + ")";
          break;
        case DOUBLE:
          result[idx] =
              "BottomPoint=("
                  + bottomTime
                  + ","
                  + doubleMinV
                  + "), "
                  + "TopPoint=("
                  + topTime
                  + ","
                  + doubleMaxV
                  + ")";
          break;
        default:
          break;
      }
    }
    // collect result
    for (int i = 0; i < w; i++) {
      long startInterval = tqs + (tqe - tqs) / w * i;
      collector.putString(startInterval, result[i]);
    }
  }
}
