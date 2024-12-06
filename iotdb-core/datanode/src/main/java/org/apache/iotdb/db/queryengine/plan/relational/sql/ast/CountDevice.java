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

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.Collections;

public class CountDevice extends AbstractQueryDeviceWithCache {

  public static final String COUNT_DEVICE_HEADER_STRING = "count(devices)";

  // For sql-input show device usage
  public CountDevice(
      final NodeLocation location, final Table table, final Expression rawExpression) {
    super(location, table, rawExpression);
  }

  @Override
  public DatasetHeader getDataSetHeader() {
    return new DatasetHeader(
        Collections.singletonList(new ColumnHeader(COUNT_DEVICE_HEADER_STRING, TSDataType.INT64)),
        true);
  }

  @Override
  public TsBlock getTsBlock(final Analysis analysis) {
    final TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeLong(results.size());
    tsBlockBuilder.declarePosition();
    return tsBlockBuilder.build();
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
