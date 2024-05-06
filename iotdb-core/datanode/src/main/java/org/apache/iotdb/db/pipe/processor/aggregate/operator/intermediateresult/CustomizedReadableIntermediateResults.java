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

package org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;

import java.util.Map;

public class CustomizedReadableIntermediateResults {
  private final Map<String, Pair<TSDataType, Object>> intermediateResults;

  public CustomizedReadableIntermediateResults(
      Map<String, Pair<TSDataType, Object>> intermediateResults) {
    this.intermediateResults = intermediateResults;
  }

  public boolean getBoolean(String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);
    if (typeResultPair.getLeft() == TSDataType.BOOLEAN) {
      return (boolean) typeResultPair.getRight();
    }
    throw new UnsupportedOperationException(
        String.format("The type %s cannot be casted to boolean.", typeResultPair.getLeft()));
  }

  public int getInt(String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case INT32:
        return (int) value;
      case INT64:
        return (int) (long) value;
      case FLOAT:
        return (int) (float) value;
      case DOUBLE:
        return (int) (double) value;
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to int.", typeResultPair.getLeft()));
    }
  }

  public long getLong(String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case INT32:
        return (int) value;
      case INT64:
        return (long) value;
      case FLOAT:
        return (long) (float) value;
      case DOUBLE:
        return (long) (double) value;
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to long.", typeResultPair.getLeft()));
    }
  }

  public float getFloat(String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case INT32:
        return (int) value;
      case INT64:
        return (long) value;
      case FLOAT:
        return (float) value;
      case DOUBLE:
        return (float) (double) value;
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to float.", typeResultPair.getLeft()));
    }
  }

  public double getDouble(String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case INT32:
        return (int) value;
      case INT64:
        return (long) value;
      case FLOAT:
        return (float) value;
      case DOUBLE:
        return (double) value;
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to double.", typeResultPair.getLeft()));
    }
  }

  // Note: This method will cast any decimal types to string without throwing
  // any exceptions.
  public String getString(String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case BOOLEAN:
        return Boolean.toString((boolean) value);
      case INT32:
        return Integer.toString((int) value);
      case INT64:
        return Long.toString((long) value);
      case FLOAT:
        return Float.toString((float) value);
      case DOUBLE:
        return Double.toString((double) value);
      case TEXT:
        return (String) value;
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to string.", typeResultPair.getLeft()));
    }
  }

  // The caller may cast the object by itself.
  public Object getObject(String key) {
    return intermediateResults.get(key).getRight();
  }

  public TSDataType getType(String key) {
    return intermediateResults.get(key).getLeft();
  }
}
