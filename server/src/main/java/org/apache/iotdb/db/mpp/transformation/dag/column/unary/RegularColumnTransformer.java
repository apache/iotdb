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

package org.apache.iotdb.db.mpp.transformation.dag.column.unary;

import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;

import java.util.regex.Pattern;

public class RegularColumnTransformer extends UnaryColumnTransformer {
  private final Pattern pattern;

  public RegularColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, Pattern pattern) {
    super(returnType, childColumnTransformer);
    this.pattern = pattern;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        returnType.writeBoolean(
            columnBuilder,
            pattern
                .matcher(childColumnTransformer.getType().getBinary(column, i).getStringValue())
                .find());
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {
    if (!childColumnTransformer.typeEquals(TypeEnum.BINARY)) {
      throw new UnsupportedOperationException(
          "Unsupported Type: " + childColumnTransformer.getType().getTypeEnum());
    }
  }
}
