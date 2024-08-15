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

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.stream.Collectors;

public class ShowDevice extends AbstractQueryDeviceWithCache {

  public ShowDevice(final QualifiedName name, final Expression rawExpression) {
    super(name, rawExpression);
  }

  public ShowDevice(final String database, final String tableName) {
    super(database, tableName);
  }

  @Override
  public DatasetHeader getDataSetHeader() {
    return new DatasetHeader(columnHeaderList, true);
  }

  @Override
  public TsBlock getTsBlock() {
    final TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(
            columnHeaderList.stream()
                .map(ColumnHeader::getColumnType)
                .collect(Collectors.toList()));
    results.forEach(
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
