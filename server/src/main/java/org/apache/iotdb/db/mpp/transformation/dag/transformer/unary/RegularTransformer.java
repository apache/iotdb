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
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.regex.Pattern;

public class RegularTransformer extends UnaryTransformer {

  private final Pattern pattern;

  public RegularTransformer(LayerPointReader layerPointReader, Pattern pattern) {
    super(layerPointReader);
    this.pattern = pattern;

    if (layerPointReaderDataType != TSDataType.TEXT) {
      throw new UnSupportedDataTypeException(
          "Unsupported data type: " + layerPointReader.getDataType().toString());
    }
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    Binary binary = layerPointReader.currentBinary();
    cachedBoolean = pattern.matcher(binary.getStringValue()).find();
  }
}
