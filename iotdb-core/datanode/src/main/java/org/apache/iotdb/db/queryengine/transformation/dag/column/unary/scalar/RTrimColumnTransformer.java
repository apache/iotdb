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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.BytesUtils;

public class RTrimColumnTransformer extends UnaryColumnTransformer {
  private final String character;

  public RTrimColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, String character) {
    super(returnType, childColumnTransformer);
    this.character = character;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        String currentValue = column.getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET);
        columnBuilder.writeBinary(BytesUtils.valueOf(rtrim(currentValue)));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  private String rtrim(String value) {
    if (value.isEmpty() || character.isEmpty()) {
      return value;
    }

    int end = value.length() - 1;

    while (end >= 0 && character.indexOf(value.charAt(end)) >= 0) end--;

    if (end < 0) {
      return "";
    }

    return value.substring(0, end + 1);
  }
}
