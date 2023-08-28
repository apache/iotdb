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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.scalar;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerPointReader;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.UnaryTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class RoundFunctionTransformer extends UnaryTransformer {
  private final TSDataType targetDataType;

  protected int places;

  public RoundFunctionTransformer(
      LayerPointReader layerPointReader, TSDataType targetDataType, int places) {
    super(layerPointReader);
    this.targetDataType = targetDataType;
    this.places = places;
  }

  @Override
  public TSDataType getDataType() {
    return targetDataType;
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    switch (layerPointReaderDataType) {
      case INT32:
        cachedDouble =
            Math.rint(layerPointReader.currentInt() * Math.pow(10, places)) / Math.pow(10, places);
        return;
      case INT64:
        cachedDouble =
            Math.rint(layerPointReader.currentLong() * Math.pow(10, places)) / Math.pow(10, places);
        return;
      case FLOAT:
        cachedDouble =
            Math.rint(layerPointReader.currentFloat() * Math.pow(10, places))
                / Math.pow(10, places);
        return;
      case DOUBLE:
        cachedDouble =
            Math.rint(layerPointReader.currentDouble() * Math.pow(10, places))
                / Math.pow(10, places);
        return;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported source dataType: %s", layerPointReaderDataType));
    }
  }
}
