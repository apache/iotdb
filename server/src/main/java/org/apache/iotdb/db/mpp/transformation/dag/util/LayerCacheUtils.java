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

package org.apache.iotdb.db.mpp.transformation.dag.util;

import org.apache.iotdb.commons.udf.utils.UDFBinaryTransformer;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.input.IUDFInputDataSet;
import org.apache.iotdb.db.mpp.transformation.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.db.mpp.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class LayerCacheUtils {

  private LayerCacheUtils() {}

  public static YieldableState yieldPoints(
      TSDataType dataType,
      LayerPointReader source,
      ElasticSerializableTVList target,
      int pointNumber)
      throws QueryProcessException, IOException {
    int count = 0;
    while (count < pointNumber) {
      final YieldableState yieldableState = yieldPoint(dataType, source, target);
      if (yieldableState != YieldableState.YIELDABLE) {
        return yieldableState;
      }
      ++count;
    }
    return YieldableState.YIELDABLE;
  }

  public static YieldableState yieldPoint(
      TSDataType dataType, LayerPointReader source, ElasticSerializableTVList target)
      throws IOException, QueryProcessException {
    final YieldableState yieldableState = source.yield();
    if (yieldableState != YieldableState.YIELDABLE) {
      return yieldableState;
    }

    if (source.isCurrentNull()) {
      target.putNull(source.currentTime());
    } else {
      switch (dataType) {
        case INT32:
          target.putInt(source.currentTime(), source.currentInt());
          break;
        case INT64:
          target.putLong(source.currentTime(), source.currentLong());
          break;
        case FLOAT:
          target.putFloat(source.currentTime(), source.currentFloat());
          break;
        case DOUBLE:
          target.putDouble(source.currentTime(), source.currentDouble());
          break;
        case BOOLEAN:
          target.putBoolean(source.currentTime(), source.currentBoolean());
          break;
        case TEXT:
          target.putBinary(
              source.currentTime(),
              UDFBinaryTransformer.transformToUDFBinary(source.currentBinary()));
          break;
        default:
          throw new UnsupportedOperationException(dataType.name());
      }
    }

    source.readyForNext();

    return YieldableState.YIELDABLE;
  }

  /** @return number of actually collected, which may be less than or equals to rowsNumber */
  public static YieldableState yieldRows(
      IUDFInputDataSet source, ElasticSerializableRowRecordList target, int rowsNumber)
      throws QueryProcessException, IOException {
    int count = 0;
    while (count < rowsNumber) {
      final YieldableState yieldableState = yieldRow(source, target);
      if (yieldableState != YieldableState.YIELDABLE) {
        return yieldableState;
      }
      ++count;
    }
    return YieldableState.YIELDABLE;
  }

  public static YieldableState yieldRow(
      IUDFInputDataSet source, ElasticSerializableRowRecordList target)
      throws IOException, QueryProcessException {
    final YieldableState yieldableState = source.canYieldNextRowInObjects();
    if (yieldableState == YieldableState.YIELDABLE) {
      target.put(source.nextRowInObjects());
    }
    return yieldableState;
  }

  /** @return number of actually collected, which may be less than or equals to pointNumber */
  public static int cachePoints(
      TSDataType dataType,
      LayerPointReader source,
      ElasticSerializableTVList target,
      int pointNumber)
      throws QueryProcessException, IOException {
    int count = 0;
    while (count < pointNumber && cachePoint(dataType, source, target)) {
      ++count;
    }
    return count;
  }

  public static boolean cachePoint(
      TSDataType dataType, LayerPointReader source, ElasticSerializableTVList target)
      throws IOException, QueryProcessException {
    if (!source.next()) {
      return false;
    }

    if (source.isCurrentNull()) {
      target.putNull(source.currentTime());
    } else {
      switch (dataType) {
        case INT32:
          target.putInt(source.currentTime(), source.currentInt());
          break;
        case INT64:
          target.putLong(source.currentTime(), source.currentLong());
          break;
        case FLOAT:
          target.putFloat(source.currentTime(), source.currentFloat());
          break;
        case DOUBLE:
          target.putDouble(source.currentTime(), source.currentDouble());
          break;
        case BOOLEAN:
          target.putBoolean(source.currentTime(), source.currentBoolean());
          break;
        case TEXT:
          target.putBinary(
              source.currentTime(),
              UDFBinaryTransformer.transformToUDFBinary(source.currentBinary()));
          break;
        default:
          throw new UnsupportedOperationException(dataType.name());
      }
    }

    source.readyForNext();

    return true;
  }

  /** @return number of actually collected, which may be less than or equals to rowsNumber */
  public static int cacheRows(
      IUDFInputDataSet source, ElasticSerializableRowRecordList target, int rowsNumber)
      throws QueryProcessException, IOException {
    int count = 0;
    while (count < rowsNumber && cacheRow(source, target)) {
      ++count;
    }
    return count;
  }

  public static boolean cacheRow(IUDFInputDataSet source, ElasticSerializableRowRecordList target)
      throws IOException, QueryProcessException {
    if (source.hasNextRowInObjects()) {
      target.put(source.nextRowInObjects());
      return true;
    } else {
      return false;
    }
  }
}
