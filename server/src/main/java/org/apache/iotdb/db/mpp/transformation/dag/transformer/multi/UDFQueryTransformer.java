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

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public abstract class UDFQueryTransformer extends Transformer {

  protected final UDTFExecutor executor;

  protected final LayerPointReader layerPointReader;
  protected final TSDataType layerPointReaderDataType;
  protected final boolean isLayerPointReaderConstant;

  protected boolean terminated;

  protected UDFQueryTransformer(UDTFExecutor executor) {
    this.executor = executor;
    layerPointReader = executor.getCollector().constructPointReaderUsingTrivialEvictionStrategy();
    layerPointReaderDataType =
        UDFDataTypeTransformer.transformToTsDataType(
            executor.getConfigurations().getOutputDataType());
    isLayerPointReaderConstant = layerPointReader.isConstantPointReader();
    terminated = false;
  }

  @Override
  public final boolean isConstantPointReader() {
    return isLayerPointReaderConstant;
  }

  @Override
  protected final YieldableState yieldValue() throws QueryProcessException, IOException {
    while (!cacheValueFromUDFOutput()) {
      final YieldableState udfYieldableState = tryExecuteUDFOnce();
      if (udfYieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
        return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
      }
      if (udfYieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA && !terminate()) {
        return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
      }
    }
    return YieldableState.YIELDABLE;
  }

  protected abstract YieldableState tryExecuteUDFOnce() throws QueryProcessException, IOException;

  @Override
  protected final boolean cacheValue() throws QueryProcessException, IOException {
    while (!cacheValueFromUDFOutput()) {
      if (!executeUDFOnce() && !terminate()) {
        return false;
      }
    }
    return true;
  }

  protected final boolean cacheValueFromUDFOutput() throws QueryProcessException, IOException {
    if (!layerPointReader.next()) {
      return false;
    }
    cachedTime = layerPointReader.currentTime();
    if (layerPointReader.isCurrentNull()) {
      currentNull = true;
    } else {
      switch (layerPointReaderDataType) {
        case INT32:
          cachedInt = layerPointReader.currentInt();
          break;
        case INT64:
          cachedLong = layerPointReader.currentLong();
          break;
        case FLOAT:
          cachedFloat = layerPointReader.currentFloat();
          break;
        case DOUBLE:
          cachedDouble = layerPointReader.currentDouble();
          break;
        case BOOLEAN:
          cachedBoolean = layerPointReader.currentBoolean();
          break;
        case TEXT:
          cachedBinary = layerPointReader.currentBinary();
          break;
        default:
          throw new UnSupportedDataTypeException(layerPointReaderDataType.toString());
      }
    }
    layerPointReader.readyForNext();
    return true;
  }

  protected abstract boolean executeUDFOnce() throws QueryProcessException, IOException;

  protected final boolean terminate() throws QueryProcessException {
    if (terminated) {
      return false;
    }
    executor.terminate();
    terminated = true;
    return true;
  }

  @Override
  public final TSDataType getDataType() {
    return layerPointReaderDataType;
  }
}
