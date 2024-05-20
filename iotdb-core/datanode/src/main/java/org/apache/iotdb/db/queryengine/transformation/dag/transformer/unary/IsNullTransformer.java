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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;

public class IsNullTransformer extends UnaryTransformer {
  private final boolean isNot;

  public IsNullTransformer(LayerReader layerReader, boolean isNot) {
    super(layerReader);
    this.isNot = isNot;
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.BOOLEAN};
  }

  /*
   * currNull = true, isNot = false -> cachedBoolean = true;
   * currNull = false, isNot = true -> cachedBoolean = true;
   * currNull = true, isNot = true -> cachedBoolean = false;
   * currNull = false, isNot = false -> cachedBoolean = false.
   * So we need use '^' here.
   */
  @Override
  protected void transform(Column[] columns, ColumnBuilder builder)
      throws QueryProcessException, IOException {
    int count = columns[0].getPositionCount();
    boolean[] isNulls = columns[0].isNull();
    for (int i = 0; i < count; i++) {
      builder.writeBoolean(isNulls[i] ^ isNot);
    }
  }
}
