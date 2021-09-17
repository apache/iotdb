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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class ArithmeticNegationTransformer extends Transformer {

  private final LayerPointReader layerPointReader;

  public ArithmeticNegationTransformer(LayerPointReader layerPointReader) {
    this.layerPointReader = layerPointReader;
  }

  @Override
  protected boolean cacheValue() throws QueryProcessException, IOException {
    if (!layerPointReader.next()) {
      return false;
    }
    cachedTime = layerPointReader.currentTime();
    switch (layerPointReader.getDataType()) {
      case INT32:
        cachedInt = -layerPointReader.currentInt();
        break;
      case INT64:
        cachedLong = -layerPointReader.currentLong();
        break;
      case FLOAT:
        cachedFloat = -layerPointReader.currentFloat();
        break;
      case DOUBLE:
        cachedDouble = -layerPointReader.currentDouble();
        break;
      default:
        throw new QueryProcessException(
            "Unsupported data type: " + layerPointReader.getDataType().toString());
    }
    layerPointReader.readyForNext();
    return true;
  }

  @Override
  public TSDataType getDataType() {
    return layerPointReader.getDataType();
  }
}
