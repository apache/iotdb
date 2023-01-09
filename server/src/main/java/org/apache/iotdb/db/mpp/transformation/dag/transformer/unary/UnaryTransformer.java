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

package org.apache.iotdb.db.mpp.transformation.dag.transformer.unary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public abstract class UnaryTransformer extends Transformer {

  protected final LayerPointReader layerPointReader;
  protected final TSDataType layerPointReaderDataType;
  protected final boolean isLayerPointReaderConstant;

  protected UnaryTransformer(LayerPointReader layerPointReader) {
    this.layerPointReader = layerPointReader;
    layerPointReaderDataType = layerPointReader.getDataType();
    isLayerPointReaderConstant = layerPointReader.isConstantPointReader();
  }

  @Override
  public final boolean isConstantPointReader() {
    return isLayerPointReaderConstant;
  }

  @Override
  public YieldableState yieldValue() throws IOException, QueryProcessException {
    final YieldableState yieldableState = layerPointReader.yield();
    if (!YieldableState.YIELDABLE.equals(yieldableState)) {
      return yieldableState;
    }

    if (!isLayerPointReaderConstant) {
      cachedTime = layerPointReader.currentTime();
    }

    if (layerPointReader.isCurrentNull()) {
      currentNull = true;
    } else {
      transformAndCache();
    }

    layerPointReader.readyForNext();
    return YieldableState.YIELDABLE;
  }

  @Override
  protected final boolean cacheValue() throws QueryProcessException, IOException {
    if (!layerPointReader.next()) {
      return false;
    }

    if (!isLayerPointReaderConstant) {
      cachedTime = layerPointReader.currentTime();
    }

    if (layerPointReader.isCurrentNull()) {
      currentNull = true;
    } else {
      transformAndCache();
    }

    layerPointReader.readyForNext();
    return true;
  }

  protected abstract void transformAndCache() throws QueryProcessException, IOException;
}
