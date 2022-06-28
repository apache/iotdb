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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

/**
 * this is a special transformer which outputs data just as input without any modification.
 *
 * <p>i.e. it's just the function f(x) = x.
 *
 * <p>It's mainly used for a UDF with aggregation query as its parameters.
 */
public class TransparentTransformer extends UnaryTransformer {

  public TransparentTransformer(LayerPointReader layerPointReader) {
    super(layerPointReader);
  }

  @Override
  public TSDataType getDataType() {
    return layerPointReaderDataType;
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    switch (layerPointReaderDataType) {
      case BOOLEAN:
        cachedBoolean = layerPointReader.currentBoolean();
        break;
      case DOUBLE:
        cachedDouble = layerPointReader.currentDouble();
        break;
      case FLOAT:
        cachedFloat = layerPointReader.currentFloat();
        break;
      case INT32:
        cachedInt = layerPointReader.currentInt();
        break;
      case INT64:
        cachedLong = layerPointReader.currentLong();
        break;
      case TEXT:
        cachedBinary = layerPointReader.currentBinary();
        break;
      default:
        throw new QueryProcessException("unsupported data type: " + layerPointReader.getDataType());
    }
  }
}
