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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.read.common.type.Type;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isCharType;

public class Like2ColumnTransformer extends BinaryColumnTransformer {

  public Like2ColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  protected void checkType() {
    if (!isCharType(leftTransformer.getType())) {
      throw new UnsupportedOperationException(
          "Unsupported Type: " + leftTransformer.getType().getTypeEnum());
    }
    if (!isCharType(rightTransformer.getType())) {
      throw new UnsupportedOperationException(
          "Unsupported Type: " + rightTransformer.getType().getTypeEnum());
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      transforme(leftColumn, rightColumn, builder, i);
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        transforme(leftColumn, rightColumn, builder, i);
      } else {
        builder.appendNull();
      }
    }
  }

  private void transforme(Column leftColumn, Column rightColumn, ColumnBuilder builder, int i) {
    if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
      LikePattern pattern =
          LikePattern.compile(
              rightColumn.getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET),
              Optional.empty(),
              false);
      builder.writeBoolean(
          pattern
              .getMatcher()
              .match(leftColumn.getBinary(i).getValues(), 0, leftColumn.getBinary(i).getLength()));
    } else {
      builder.appendNull();
    }
  }
}
