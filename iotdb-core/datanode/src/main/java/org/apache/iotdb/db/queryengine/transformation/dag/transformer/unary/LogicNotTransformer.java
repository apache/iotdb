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
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;

public class LogicNotTransformer extends UnaryTransformer {

  public LogicNotTransformer(LayerReader layerReader) {
    super(layerReader);

    if (layerReaderDataType != TSDataType.BOOLEAN) {
      throw new UnSupportedDataTypeException(
          "Unsupported data type: " + layerReader.getDataTypes()[0].toString());
    }
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.BOOLEAN};
  }

  @Override
  protected void transform(Column[] columns, ColumnBuilder builder)
      throws QueryProcessException, IOException {
    int count = columns[0].getPositionCount();
    boolean[] values = columns[0].getBooleans();
    boolean[] isNulls = columns[0].isNull();

    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        boolean res = !values[i];
        builder.writeBoolean(res);
      } else {
        builder.appendNull();
      }
    }
  }
}
