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

package org.apache.iotdb.library.util;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.type.Binary;
import org.apache.iotdb.udf.api.type.Type;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Minimal Row / PointCollector / parameter fakes shared by library-udf tests. */
public class UdfTestFakes {

  private UdfTestFakes() {}

  /** A single-column DOUBLE row. */
  public static Row rowOf(long timestamp, double value) {
    return new DoubleRow(timestamp, value);
  }

  public static UDFParameters parameters(Map<String, String> attributes) {
    return new UDFParameters(
        java.util.Arrays.asList("s0"),
        java.util.Collections.singletonList(Type.DOUBLE),
        new HashMap<>(attributes),
        new HashMap<>());
  }

  public static UDTFConfigurations configurations() {
    return new UDTFConfigurations(ZoneId.systemDefault());
  }

  /** PointCollector that records int and double outputs keyed by timestamp, in emission order. */
  public static class RecordingPointCollector implements PointCollector {

    private final Map<Long, Integer> ints = new LinkedHashMap<>();
    private final Map<Long, Double> doubles = new LinkedHashMap<>();

    public Map<Long, Integer> getInts() {
      return ints;
    }

    public Map<Long, Double> getDoubles() {
      return doubles;
    }

    public int totalPoints() {
      return ints.size() + doubles.size();
    }

    @Override
    public void putInt(long timestamp, int value) {
      ints.put(timestamp, value);
    }

    @Override
    public void putLong(long timestamp, long value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putFloat(long timestamp, float value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putDouble(long timestamp, double value) {
      doubles.put(timestamp, value);
    }

    @Override
    public void putBoolean(long timestamp, boolean value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putBinary(long timestamp, Binary value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putString(long timestamp, String value) {
      throw new UnsupportedOperationException();
    }
  }

  private static class DoubleRow implements Row {

    private final long timestamp;
    private final double value;

    private DoubleRow(long timestamp, double value) {
      this.timestamp = timestamp;
      this.value = value;
    }

    @Override
    public long getTime() {
      return timestamp;
    }

    @Override
    public int getInt(int columnIndex) {
      return (int) value;
    }

    @Override
    public long getLong(int columnIndex) {
      return (long) value;
    }

    @Override
    public float getFloat(int columnIndex) {
      return (float) value;
    }

    @Override
    public double getDouble(int columnIndex) {
      return value;
    }

    @Override
    public boolean getBoolean(int columnIndex) {
      return value != 0;
    }

    @Override
    public Binary getBinary(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int columnIndex) {
      return String.valueOf(value);
    }

    @Override
    public Type getDataType(int columnIndex) {
      return Type.DOUBLE;
    }

    @Override
    public boolean isNull(int columnIndex) {
      return false;
    }

    @Override
    public int size() {
      return 1;
    }
  }
}
