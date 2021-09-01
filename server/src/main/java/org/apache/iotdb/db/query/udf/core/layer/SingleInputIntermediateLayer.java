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

package org.apache.iotdb.db.query.udf.core.layer;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.core.layer.SafetyLine.SafetyPile;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

public class SingleInputIntermediateLayer implements IntermediateLayer {

  private static final int CACHE_BLOCK_SIZE = 1;

  private final TSDataType dataType;
  private final LayerPointReader parentLayerPointReader;
  private final ElasticSerializableTVList tvList;
  private final SafetyLine safetyLine;

  public SingleInputIntermediateLayer(
      LayerPointReader parentLayerPointReader, long queryId, float memoryBudgetInMB)
      throws QueryProcessException {
    this.parentLayerPointReader = parentLayerPointReader;
    dataType = parentLayerPointReader.getDataType();
    tvList =
        ElasticSerializableTVList.newElasticSerializableTVList(
            dataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    safetyLine = new SafetyLine();
  }

  @Override
  public LayerPointReader constructPointReader() {

    return new LayerPointReader() {

      private final SafetyPile safetyPile = safetyLine.addSafetyPile();

      private boolean hasCached = false;
      private int currentPointIndex = -1;

      @Override
      public boolean next() throws QueryProcessException, IOException {
        if (hasCached) {
          return true;
        }

        if (currentPointIndex < tvList.size() - 1) {
          ++currentPointIndex;
          hasCached = true;
        }

        // tvList.size() - 1 <= currentPointIndex
        if (!hasCached && parentLayerPointReader.next()) {
          cachePoint();
          parentLayerPointReader.readyForNext();

          ++currentPointIndex;
          hasCached = true;
        }

        return hasCached;
      }

      private void cachePoint() throws IOException, QueryProcessException {
        switch (dataType) {
          case INT32:
            tvList.putInt(
                parentLayerPointReader.currentTime(), parentLayerPointReader.currentInt());
            break;
          case INT64:
            tvList.putLong(
                parentLayerPointReader.currentTime(), parentLayerPointReader.currentLong());
            break;
          case FLOAT:
            tvList.putFloat(
                parentLayerPointReader.currentTime(), parentLayerPointReader.currentFloat());
            break;
          case DOUBLE:
            tvList.putDouble(
                parentLayerPointReader.currentTime(), parentLayerPointReader.currentDouble());
            break;
          case BOOLEAN:
            tvList.putBoolean(
                parentLayerPointReader.currentTime(), parentLayerPointReader.currentBoolean());
            break;
          case TEXT:
            tvList.putBinary(
                parentLayerPointReader.currentTime(), parentLayerPointReader.currentBinary());
            break;
          default:
            throw new UnsupportedOperationException(dataType.name());
        }
      }

      @Override
      public void readyForNext() {
        hasCached = false;

        safetyPile.moveForwardTo(currentPointIndex + 1);
        // todo: reduce the update rate
        tvList.setEvictionUpperBound(safetyLine.getSafetyLine());
      }

      @Override
      public TSDataType getDataType() {
        return dataType;
      }

      @Override
      public long currentTime() throws IOException {
        return tvList.getTime(currentPointIndex);
      }

      @Override
      public int currentInt() throws IOException {
        return tvList.getInt(currentPointIndex);
      }

      @Override
      public long currentLong() throws IOException {
        return tvList.getLong(currentPointIndex);
      }

      @Override
      public float currentFloat() throws IOException {
        return tvList.getFloat(currentPointIndex);
      }

      @Override
      public double currentDouble() throws IOException {
        return tvList.getDouble(currentPointIndex);
      }

      @Override
      public boolean currentBoolean() throws IOException {
        return tvList.getBoolean(currentPointIndex);
      }

      @Override
      public Binary currentBinary() throws IOException {
        return tvList.getBinary(currentPointIndex);
      }
    };
  }
}
