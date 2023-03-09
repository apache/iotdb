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

package org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.scalar;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.expression.multi.builtin.helper.CastHelper;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.UnaryTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

public class CastFunctionTransformer extends UnaryTransformer {
  private final TSDataType targetDataType;

  public CastFunctionTransformer(LayerPointReader layerPointReader, TSDataType targetDataType) {
    super(layerPointReader);
    this.targetDataType = targetDataType;
  }

  @Override
  public TSDataType getDataType() {
    return targetDataType;
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    switch (layerPointReaderDataType) {
      case INT32:
        cast(layerPointReader.currentInt());
        return;
      case INT64:
        cast(layerPointReader.currentLong());
        return;
      case FLOAT:
        cast(layerPointReader.currentFloat());
        return;
      case DOUBLE:
        cast(layerPointReader.currentDouble());
        return;
      case BOOLEAN:
        cast(layerPointReader.currentBoolean());
        return;
      case TEXT:
        cast(layerPointReader.currentBinary());
        return;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported source dataType: %s", layerPointReaderDataType));
    }
  }

  private void cast(int value) {
    switch (targetDataType) {
      case INT32:
        cachedInt = value;
        return;
      case INT64:
        cachedLong = value;
        return;
      case FLOAT:
        cachedFloat = value;
        return;
      case DOUBLE:
        cachedDouble = value;
        return;
      case BOOLEAN:
        cachedBoolean = (value != 0);
        return;
      case TEXT:
        cachedBinary = Binary.valueOf(String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerPointReaderDataType));
    }
  }

  private void cast(long value) {
    switch (targetDataType) {
      case INT32:
        cachedInt = CastHelper.castLongToInt(value);
        return;
      case INT64:
        cachedLong = value;
        return;
      case FLOAT:
        cachedFloat = value;
        return;
      case DOUBLE:
        cachedDouble = value;
        return;
      case BOOLEAN:
        cachedBoolean = (value != 0L);
        return;
      case TEXT:
        cachedBinary = Binary.valueOf(String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerPointReaderDataType));
    }
  }

  private void cast(float value) {
    switch (targetDataType) {
      case INT32:
        cachedInt = CastHelper.castFloatToInt(value);
        return;
      case INT64:
        cachedLong = CastHelper.castFloatToLong(value);
        return;
      case FLOAT:
        cachedFloat = value;
        return;
      case DOUBLE:
        cachedDouble = value;
        return;
      case BOOLEAN:
        cachedBoolean = (value != 0f);
        return;
      case TEXT:
        cachedBinary = Binary.valueOf(String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerPointReaderDataType));
    }
  }

  private void cast(double value) {
    switch (targetDataType) {
      case INT32:
        cachedInt = CastHelper.castDoubleToInt(value);
        return;
      case INT64:
        cachedLong = CastHelper.castDoubleToLong(value);
        return;
      case FLOAT:
        cachedFloat = CastHelper.castDoubleToFloat(value);
        return;
      case DOUBLE:
        cachedDouble = value;
        return;
      case BOOLEAN:
        cachedBoolean = (value != 0.0);
        return;
      case TEXT:
        cachedBinary = Binary.valueOf(String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerPointReaderDataType));
    }
  }

  private void cast(boolean value) {
    switch (targetDataType) {
      case INT32:
        cachedInt = value ? 1 : 0;
        return;
      case INT64:
        cachedLong = value ? 1L : 0;
        return;
      case FLOAT:
        cachedFloat = value ? 1.0f : 0;
        return;
      case DOUBLE:
        cachedDouble = value ? 1.0 : 0;
        return;
      case BOOLEAN:
        cachedBoolean = value;
        return;
      case TEXT:
        cachedBinary = Binary.valueOf(String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerPointReaderDataType));
    }
  }

  private void cast(Binary value) {
    String stringValue = value.getStringValue();
    // could throw exception when parsing string value
    switch (targetDataType) {
      case INT32:
        cachedInt = Integer.parseInt(stringValue);
        return;
      case INT64:
        cachedLong = Long.parseLong(stringValue);
        return;
      case FLOAT:
        cachedFloat = CastHelper.castTextToFloat(stringValue);
        return;
      case DOUBLE:
        cachedDouble = CastHelper.castTextToDouble(stringValue);
        return;
      case BOOLEAN:
        cachedBoolean = CastHelper.castTextToBoolean(stringValue);
        return;
      case TEXT:
        cachedBinary = Binary.valueOf(String.valueOf(value));
        return;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported target dataType: %s", layerPointReaderDataType));
    }
  }
}
