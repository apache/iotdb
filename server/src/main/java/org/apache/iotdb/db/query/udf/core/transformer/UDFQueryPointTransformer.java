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

package org.apache.iotdb.db.query.udf.core.transformer;

import org.apache.iotdb.db.query.udf.core.access.PointImpl;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;

public class UDFQueryPointTransformer extends UDFQueryTransformer {

  protected final LayerPointReader layerPointReader;

  protected final PointImpl cachedPoint;

  public UDFQueryPointTransformer(LayerPointReader layerPointReader, UDTFExecutor executor) {
    super(executor);
    this.layerPointReader = layerPointReader;
    cachedPoint = new PointImpl();
  }

  @Override
  protected boolean executeUDFOnce() throws Exception {
    if (!layerPointReader.next()) {
      return false;
    }
    cachedPoint.setCurrentTime(layerPointReader.currentTime());
    switch (layerPointReader.getDataType()) {
      case INT32:
        cachedPoint.setCurrentInt(layerPointReader.currentInt());
        break;
      case INT64:
        cachedPoint.setCurrentLong(layerPointReader.currentLong());
        break;
      case FLOAT:
        cachedPoint.setCurrentFloat(layerPointReader.currentFloat());
        break;
      case DOUBLE:
        cachedPoint.setCurrentDouble(layerPointReader.currentDouble());
        break;
      case BOOLEAN:
        cachedPoint.setCurrentBoolean(layerPointReader.currentBoolean());
        break;
      case TEXT:
        cachedPoint.setCurrentBinary(layerPointReader.currentBinary());
        break;
      default:
        throw new UnSupportedDataTypeException(udfOutputDataType.toString());
    }
    layerPointReader.readyForNext();
    executor.execute(cachedPoint);
    return true;
  }
}
