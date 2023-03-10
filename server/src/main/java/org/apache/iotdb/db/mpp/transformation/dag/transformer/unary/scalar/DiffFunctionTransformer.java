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
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.UnaryTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class DiffFunctionTransformer extends UnaryTransformer {
  private final boolean ignoreNull;

  // cache the last non-null value
  private double lastValue;

  // indicate whether lastValue is null
  private boolean lastValueIsNull = true;

  public DiffFunctionTransformer(LayerPointReader layerPointReader, boolean ignoreNull) {
    super(layerPointReader);
    this.ignoreNull = ignoreNull;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.DOUBLE;
  }

  @Override
  public final YieldableState yieldValue() throws IOException, QueryProcessException {
    final YieldableState yieldableState = layerPointReader.yield();
    if (!YieldableState.YIELDABLE.equals(yieldableState)) {
      return yieldableState;
    }

    if (!isLayerPointReaderConstant) {
      cachedTime = layerPointReader.currentTime();
    }

    transformAndCache();

    layerPointReader.readyForNext();
    return YieldableState.YIELDABLE;
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    if (layerPointReader.isCurrentNull()) {
      currentNull = true; // currValue is null, append null

      // When currValue is null:
      // ignoreNull = true, keep lastValueIsNull as before
      // ignoreNull = false, update lastValueIsNull to true
      lastValueIsNull |= !ignoreNull;
    } else {
      double currValue;
      switch (layerPointReaderDataType) {
        case INT32:
          currValue = layerPointReader.currentInt();
          break;
        case INT64:
          currValue = layerPointReader.currentLong();
          break;
        case FLOAT:
          currValue = layerPointReader.currentFloat();
          break;
        case DOUBLE:
          currValue = layerPointReader.currentDouble();
          break;
        default:
          throw new QueryProcessException(
              "Unsupported data type: " + layerPointReader.getDataType().toString());
      }
      if (lastValueIsNull) {
        currentNull = true; // lastValue is null, append null
      } else {
        cachedDouble = currValue - lastValue;
      }

      lastValue = currValue; // currValue is not null, update lastValue
      lastValueIsNull = false;
    }
  }
}
