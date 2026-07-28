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
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;

import java.util.Map;

import static org.apache.iotdb.db.utils.TypeServices.Pipe.CUSTOMIZED_INTERMEDIATE_RESULT_TO_DOUBLE_SERVICE;
import static org.apache.iotdb.db.utils.TypeServices.Pipe.CUSTOMIZED_INTERMEDIATE_RESULT_TO_FLOAT_SERVICE;
import static org.apache.iotdb.db.utils.TypeServices.Pipe.CUSTOMIZED_INTERMEDIATE_RESULT_TO_INT_SERVICE;
import static org.apache.iotdb.db.utils.TypeServices.Pipe.CUSTOMIZED_INTERMEDIATE_RESULT_TO_LONG_SERVICE;
import static org.apache.iotdb.db.utils.TypeServices.Pipe.CUSTOMIZED_INTERMEDIATE_RESULT_TO_STRING_SERVICE;

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

    return CUSTOMIZED_INTERMEDIATE_RESULT_TO_INT_SERVICE
        .call(getType(typeResultPair.getLeft(), "int"))
        .applyAsInt(typeResultPair.getRight());
  }

  public long getLong(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    return CUSTOMIZED_INTERMEDIATE_RESULT_TO_LONG_SERVICE
        .call(getType(typeResultPair.getLeft(), "long"))
        .applyAsLong(typeResultPair.getRight());
  }

  public float getFloat(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    return CUSTOMIZED_INTERMEDIATE_RESULT_TO_FLOAT_SERVICE
        .call(getType(typeResultPair.getLeft(), "float"))
        .apply(typeResultPair.getRight());
  }

  public double getDouble(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    return CUSTOMIZED_INTERMEDIATE_RESULT_TO_DOUBLE_SERVICE
        .call(getType(typeResultPair.getLeft(), "double"))
        .applyAsDouble(typeResultPair.getRight());
  }

  // Note: This method will cast any decimal types to string without throwing
  // any exceptions.
  public String getString(final String key) {
    final Pair<TSDataType, Object> typeResultPair = intermediateResults.get(key);

    return CUSTOMIZED_INTERMEDIATE_RESULT_TO_STRING_SERVICE
        .call(getType(typeResultPair.getLeft(), "string"))
        .apply(typeResultPair.getRight());
  }

  private static Type getType(final TSDataType dataType, final String targetType) {
    try {
      return Type.fromTsDataType(dataType);
    } catch (final UnsupportedOperationException ignored) {
      throw new UnsupportedOperationException(
          String.format("The type %s cannot be casted to %s.", dataType, targetType));
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
