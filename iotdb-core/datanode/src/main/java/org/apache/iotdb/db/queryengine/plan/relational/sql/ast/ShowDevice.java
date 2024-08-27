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
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.TableDeviceQuerySource;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.Objects;
import java.util.stream.Collectors;

public class ShowDevice extends AbstractQueryDeviceWithCache {
  private Offset offset;
  private Node limit;

  public ShowDevice(
      final NodeLocation location,
      final Table table,
      final Expression rawExpression,
      final Offset offset,
      final Node limit) {
    super(location, table, rawExpression);
    this.offset = offset;
    this.limit = limit;
  }

  public ShowDevice(final String database, final String tableName) {
    super(database, tableName);
  }

  public Offset getOffset() {
    return offset;
  }

  public Node getLimit() {
    return limit;
  }

  @Override
  public DatasetHeader getDataSetHeader() {
    return new DatasetHeader(columnHeaderList, true);
  }

  @Override
  public TsBlock getTsBlock(final Analysis analysis) {
    final TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(
            columnHeaderList.stream()
                .map(ColumnHeader::getColumnType)
                .collect(Collectors.toList()));

    final int startIndex;
    if (Objects.nonNull(offset)) {
      startIndex =
          Math.min(
              analysis.getOffset(offset) > Integer.MAX_VALUE
                  ? Integer.MAX_VALUE
                  : (int) analysis.getOffset(offset),
              results.size());
    } else {
      startIndex = 0;
    }

    final int endIndex;
    if (Objects.nonNull(limit)) {
      final long limitLong = analysis.getLimit(limit).orElse(-1);
      endIndex =
          limitLong < 0 || (startIndex + limitLong) > results.size()
              ? results.size()
              : (int) (limitLong + startIndex);
    } else {
      endIndex = results.size();
    }

    results
        .subList(startIndex, endIndex)
        .forEach(
            result ->
                TableDeviceQuerySource.transformToTsBlockColumns(
                    result, tsBlockBuilder, database, tableName, columnHeaderList, 1));
    return tsBlockBuilder.build();
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitShowDevice(this, context);
  }

  @Override
  public String toString() {
    return "ShowDevice" + toStringContent();
  }
}
