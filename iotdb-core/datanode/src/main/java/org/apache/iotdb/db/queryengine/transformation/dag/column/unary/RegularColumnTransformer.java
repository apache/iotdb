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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.Type;

import java.util.regex.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isCharType;

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
                .matcher(
                    childColumnTransformer
                        .getType()
                        .getBinary(column, i)
                        .getStringValue(TSFileConfig.STRING_CHARSET))
                .find());
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i] && !column.isNull(i)) {
        returnType.writeBoolean(
            columnBuilder,
            pattern
                .matcher(
                    childColumnTransformer
                        .getType()
                        .getBinary(column, i)
                        .getStringValue(TSFileConfig.STRING_CHARSET))
                .find());
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {
    if (!isCharType(childColumnTransformer.getType())) {
      throw new UnsupportedOperationException(
          "Unsupported Type: " + childColumnTransformer.getType().getTypeEnum());
    }
  }
}
