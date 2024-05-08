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

package org.apache.iotdb.db.mpp.transformation.dag.transformer.multi;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerRowReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.udf.api.access.Row;

import java.io.IOException;
import java.util.Objects;

public class MappableUDFQueryRowTransformer extends UDFQueryTransformer {

  protected final LayerRowReader layerRowReader;
  private final boolean isLayerRowReaderConstant;

  public MappableUDFQueryRowTransformer(LayerRowReader layerRowReader, UDTFExecutor executor) {
    super(executor);
    this.layerRowReader = layerRowReader;
    this.isLayerRowReaderConstant = false;
  }

  @Override
  protected boolean cacheValue() throws QueryProcessException, IOException {
    if (!layerRowReader.next()) {
      return false;
    }

    if (!isLayerRowReaderConstant) {
      cachedTime = layerRowReader.currentTime();
    }

    if (layerRowReader.isCurrentNull()) {
      currentNull = true;
    } else {
      Row row = layerRowReader.currentRow();
      execute(row);
    }

    layerRowReader.readyForNext();
    return true;
  }

  @Override
  protected YieldableState yieldValue() throws QueryProcessException, IOException {
    final YieldableState yieldableState = layerRowReader.yield();
    if (!YieldableState.YIELDABLE.equals(yieldableState)) {
      return yieldableState;
    }

    if (!isLayerRowReaderConstant) {
      cachedTime = layerRowReader.currentTime();
    }

    if (layerRowReader.isCurrentNull()) {
      currentNull = true;
    } else {
      Row row = layerRowReader.currentRow();
      execute(row);
    }

    layerRowReader.readyForNext();
    return YieldableState.YIELDABLE;
  }

  private void execute(Row row) {
    executor.execute(row);
    Object currentValue = executor.getCurrentValue();
    if (Objects.isNull(currentValue)) {
      currentNull = true;
      return;
    }
    switch (tsDataType) {
      case INT32:
        cachedInt = (int) currentValue;
        break;
      case INT64:
        cachedLong = (long) currentValue;
        break;
      case FLOAT:
        cachedFloat = (float) currentValue;
        break;
      case DOUBLE:
        cachedDouble = (double) currentValue;
        break;
      case BOOLEAN:
        cachedBoolean = (boolean) currentValue;
        break;
      case TEXT:
        cachedBinary = (Binary) currentValue;
        break;
      default:
        throw new UnSupportedDataTypeException(tsDataType.toString());
    }
  }

  @Override
  public boolean isConstantPointReader() {
    return isLayerRowReaderConstant;
  }
}
