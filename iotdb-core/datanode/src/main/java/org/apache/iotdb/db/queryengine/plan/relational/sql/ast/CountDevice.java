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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.Collections;

public class CountDevice extends AbstractQueryDeviceWithCache {

  // For sql-input show device usage
  public CountDevice(final String tableName, final Expression rawExpression) {
    super(tableName, rawExpression);
  }

  @Override
  public void setColumnHeaderList() {
    columnHeaderList =
        Collections.singletonList(new ColumnHeader("count(devices)", TSDataType.INT64));
  }

  @Override
  protected void buildTsBlock(final TsBlockBuilder tsBlockBuilder) {
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeLong(results.size());
    tsBlockBuilder.declarePosition();
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCountDevice(this, context);
  }

  @Override
  public String toString() {
    return "CountDevice" + toStringContent();
  }
}
