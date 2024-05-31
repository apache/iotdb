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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.UnaryTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public class ReplaceFunctionTransformer extends UnaryTransformer {
  private final String from;
  private final String to;

  public ReplaceFunctionTransformer(LayerReader layerReader, String from, String to) {
    super(layerReader);
    this.from = from;
    this.to = to;

    if (layerReaderDataType != TSDataType.TEXT) {
      throw new UnSupportedDataTypeException("Unsupported data type: " + layerReaderDataType);
    }
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.TEXT};
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();

    Binary[] binaries = columns[0].getBinaries();
    boolean[] isNulls = columns[0].isNull();
    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        String res = binaries[i].getStringValue(TSFileConfig.STRING_CHARSET).replace(from, to);
        Binary bin = BytesUtils.valueOf(res);
        builder.writeBinary(bin);
      } else {
        builder.appendNull();
      }
    }
  }
}
