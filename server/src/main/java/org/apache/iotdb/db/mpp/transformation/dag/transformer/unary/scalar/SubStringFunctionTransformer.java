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
import org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.UnaryTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

import static org.apache.iotdb.db.mpp.transformation.dag.column.unary.scalar.SubStringFunctionColumnTransformer.EMPTY_STRING;

public class SubStringFunctionTransformer extends UnaryTransformer {
  private int beginPosition;
  private int endPosition;

  public SubStringFunctionTransformer(
      LayerPointReader layerPointReader, int beginPosition, int length) {
    super(layerPointReader);
    this.endPosition =
        (length == Integer.MAX_VALUE ? Integer.MAX_VALUE : beginPosition + length - 1);
    this.beginPosition = beginPosition > 0 ? beginPosition - 1 : 0;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.TEXT;
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    String currentValue = layerPointReader.currentBinary().getStringValue();
    if (beginPosition >= currentValue.length() || endPosition < 0) {
      currentValue = EMPTY_STRING;
    } else {
      if (endPosition >= currentValue.length()) {
        currentValue = currentValue.substring(beginPosition);
      } else {
        currentValue = currentValue.substring(beginPosition, endPosition);
      }
    }
    cachedBinary = Binary.valueOf(currentValue);
  }
}
