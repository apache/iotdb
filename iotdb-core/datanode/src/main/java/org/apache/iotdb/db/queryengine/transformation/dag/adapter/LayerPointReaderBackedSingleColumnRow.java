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

package org.apache.iotdb.db.queryengine.transformation.dag.adapter;

import org.apache.iotdb.commons.udf.utils.UDFBinaryTransformer;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.queryengine.transformation.api.LayerPointReader;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.type.Binary;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

public class LayerPointReaderBackedSingleColumnRow implements Row {

  private final LayerPointReader layerPointReader;

  public LayerPointReaderBackedSingleColumnRow(LayerPointReader layerPointReader) {
    this.layerPointReader = layerPointReader;
  }

  @Override
  public long getTime() throws IOException {
    return layerPointReader.currentTime();
  }

  @Override
  public int getInt(int columnIndex) throws IOException {
    return layerPointReader.currentInt();
  }

  @Override
  public long getLong(int columnIndex) throws IOException {
    return layerPointReader.currentLong();
  }

  @Override
  public float getFloat(int columnIndex) throws IOException {
    return layerPointReader.currentFloat();
  }

  @Override
  public double getDouble(int columnIndex) throws IOException {
    return layerPointReader.currentDouble();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws IOException {
    return layerPointReader.currentBoolean();
  }

  @Override
  public Binary getBinary(int columnIndex) throws IOException {
    return UDFBinaryTransformer.transformToUDFBinary(layerPointReader.currentBinary());
  }

  @Override
  public String getString(int columnIndex) throws IOException {
    return layerPointReader.currentBinary().getStringValue();
  }

  @Override
  public Type getDataType(int columnIndex) {
    return UDFDataTypeTransformer.transformToUDFDataType(layerPointReader.getDataType());
  }

  @Override
  public boolean isNull(int columnIndex) {
    try {
      return layerPointReader.isCurrentNull();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int size() {
    return 1;
  }
}
