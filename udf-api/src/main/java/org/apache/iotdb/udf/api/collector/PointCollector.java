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

package org.apache.iotdb.udf.api.collector;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.type.Binary;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

/**
 * Used to collect time series data points generated by {@link UDTF#transform(Row, PointCollector)},
 * Notice that one timestamp can not be put in the PointCollector more than once, or it may stop the
 * calculation. {@link UDTF#transform(RowWindow, PointCollector)} or {@link
 * UDTF#terminate(PointCollector)}.
 */
public interface PointCollector {

  /**
   * Collects an int data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code Type.INT32} by calling {@link UDTFConfigurations#setOutputDataType(Type)} in {@link
   * UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value int value to collect
   * @throws IOException if any I/O errors occur
   * @see Type
   */
  void putInt(long timestamp, int value) throws IOException;

  /**
   * Collects a long data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code Type.INT64} by calling {@link UDTFConfigurations#setOutputDataType(Type)} in {@link
   * UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value long value to collect
   * @throws IOException if any I/O errors occur
   * @see Type
   */
  void putLong(long timestamp, long value) throws IOException;

  /**
   * Collects a float data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.FLOAT} by calling {@link UDTFConfigurations#setOutputDataType(Type)} in
   * {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value float value to collect
   * @throws IOException if any I/O errors occur
   * @see Type
   */
  void putFloat(long timestamp, float value) throws IOException;

  /**
   * Collects a double data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.DOUBLE} by calling {@link UDTFConfigurations#setOutputDataType(Type)} in
   * {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value double value to collect
   * @throws IOException if any I/O errors occur
   * @see Type
   */
  void putDouble(long timestamp, double value) throws IOException;

  /**
   * Collects a boolean data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code TSDataType.BOOLEAN} by calling {@link UDTFConfigurations#setOutputDataType(Type)} in
   * {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value boolean value to collect
   * @throws IOException if any I/O errors occur
   * @see Type
   */
  void putBoolean(long timestamp, boolean value) throws IOException;

  /**
   * Collects a Binary data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code Type.TEXT} by calling {@link UDTFConfigurations#setOutputDataType(Type)} in {@link
   * UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value Binary value to collect
   * @throws IOException if any I/O errors occur
   * @throws RuntimeException if memory is not enough to continue collecting data points
   * @see Type
   */
  void putBinary(long timestamp, Binary value) throws IOException;

  /**
   * Collects a String data point with timestamp.
   *
   * <p>Before calling this method, you need to ensure that the UDF output data type is set to
   * {@code Type.TEXT} by calling {@link UDTFConfigurations#setOutputDataType(Type)} in {@link
   * UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
   *
   * @param timestamp timestamp to collect
   * @param value String value to collect
   * @throws IOException if any I/O errors occur
   * @throws RuntimeException if memory is not enough to continue collecting data points
   * @see Type
   */
  void putString(long timestamp, String value) throws IOException;
}
