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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.pipe.api.type.Binary;
import org.apache.iotdb.rpc.RpcUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;

public class CustomizedReadableIntermediateResults {
  private final Map<String, Pair<TSDataType, Object>> intermediateResults;

  public CustomizedReadableIntermediateResults(
      final Map<String, Pair<TSDataType, Object>> intermediateResults) {
    this.intermediateResults = intermediateResults;
  }

  public boolean getBoolean(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);
    if (typeResultPair.getLeft() == TSDataType.BOOLEAN) {
      return (boolean) typeResultPair.getRight();
    }
    throw new UnsupportedOperationException(
        String.format("The type %s cannot be casted to boolean.", typeResultPair.getLeft()));
  }

  public int getInt(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case INT32:
        return (int) value;
      case DATE:
        return DateUtils.parseDateExpressionToInt((LocalDate) value);
      case INT64:
      case TIMESTAMP:
        return (int) (long) value;
      case FLOAT:
        return (int) (float) value;
      case DOUBLE:
        return (int) (double) value;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      case STRING:
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to int.", typeResultPair.getLeft()));
    }
  }

  public long getLong(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case INT32:
        return (int) value;
      case DATE:
        return DateUtils.parseDateExpressionToInt((LocalDate) value);
      case INT64:
      case TIMESTAMP:
        return (long) value;
      case FLOAT:
        return (long) (float) value;
      case DOUBLE:
        return (long) (double) value;
      case BOOLEAN:
      case STRING:
      case TEXT:
      case BLOB:
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to long.", typeResultPair.getLeft()));
    }
  }

  public float getFloat(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case INT32:
        return (int) value;
      case DATE:
        return DateUtils.parseDateExpressionToInt((LocalDate) value);
      case INT64:
      case TIMESTAMP:
        return (long) value;
      case FLOAT:
        return (float) value;
      case DOUBLE:
        return (float) (double) value;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      case STRING:
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to float.", typeResultPair.getLeft()));
    }
  }

  public double getDouble(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case INT32:
        return (int) value;
      case DATE:
        return DateUtils.parseDateExpressionToInt((LocalDate) value);
      case INT64:
      case TIMESTAMP:
        return (long) value;
      case FLOAT:
        return (float) value;
      case DOUBLE:
        return (double) value;
      case BOOLEAN:
      case STRING:
      case TEXT:
      case BLOB:
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to double.", typeResultPair.getLeft()));
    }
  }

  // Note: This method will cast any decimal types to string without throwing
  // any exceptions.
  public String getString(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    final TSDataType type = typeResultPair.getLeft();
    final Object value = typeResultPair.getRight();
    switch (type) {
      case BOOLEAN:
        return Boolean.toString((boolean) value);
      case INT32:
        return Integer.toString((int) value);
      case DATE:
        return ((LocalDate) value).toString();
      case INT64:
        return Long.toString((long) value);
      case TIMESTAMP:
        return RpcUtils.formatDatetime(
            RpcUtils.DEFAULT_TIME_FORMAT,
            CommonDescriptor.getInstance().getConfig().getTimestampPrecision(),
            (long) value,
            ZoneId.systemDefault());
      case FLOAT:
        return Float.toString((float) value);
      case DOUBLE:
        return Double.toString((double) value);
      case TEXT:
      case STRING:
        return (String) value;
      case BLOB:
        return BytesUtils.parseBlobByteArrayToString(((Binary) value).getValues());
      default:
        throw new UnsupportedOperationException(
            String.format("The type %s cannot be casted to string.", typeResultPair.getLeft()));
    }
  }

  // The caller may cast the object by itself.
  public Object getObject(final String key) {
    return intermediateResults.get(key).getRight();
  }

  public TSDataType getType(final String key) {
    return intermediateResults.get(key).getLeft();
  }
}
