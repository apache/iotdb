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

package org.apache.iotdb.db.mpp.transformation.dag.transformer.other;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.List;

public class CaseWhenThenTransformer extends Transformer {
  List<LayerPointReader> whenList;
  List<LayerPointReader> thenList;
  LayerPointReader elseLayerPointReader;

  TSDataType type = null;

  public CaseWhenThenTransformer(
      List<LayerPointReader> whenList,
      List<LayerPointReader> thenList,
      LayerPointReader elseLayerPointReader) {
    this.whenList = whenList;
    this.thenList = thenList;
    this.elseLayerPointReader = elseLayerPointReader;
  }

  @Override
  public boolean isConstantPointReader() {
    return false;
  }

  // Type check should have been done in FE,
  // so now thenList[0] can represent the type of entire CASE expression.
  @Override
  public TSDataType getDataType() {
    if (type == null) {
      LayerPointReader first = thenList.get(0);
      if (first.getDataType() == TSDataType.BOOLEAN || first.getDataType() == TSDataType.TEXT) {
        type = first.getDataType();
      } else if (first.getDataType().isNumeric()) {
        type = TSDataType.DOUBLE;
      } else {
        throw new UnsupportedOperationException(first.getDataType().name());
      }
    }
    return type;
  }

  @Override
  protected boolean cacheValue() throws QueryProcessException, IOException {
    return false;
  }

  @Override
  protected YieldableState yieldValue() throws Exception {
    return null;
  }
}
