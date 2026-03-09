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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IntoNode extends SingleChildProcessNode {
  private final String database;
  private final String table;
  private final List<ColumnSchema> columns;
  private final List<Symbol> neededInputColumnNames;
  private final Symbol rowCountSymbol;

  public IntoNode(
      PlanNodeId id,
      PlanNode child,
      String database,
      String table,
      List<ColumnSchema> columns,
      List<Symbol> neededInputColumnNames,
      Symbol rowCountSymbol) {
    super(id, child);
    this.database = database;
    this.table = table;
    this.columns = columns;
    this.neededInputColumnNames = neededInputColumnNames;
    if (columns.size() != neededInputColumnNames.size()) {
      throw new IllegalArgumentException(
          "insert into table columns's size should be same as query result");
    }
    this.rowCountSymbol = rowCountSymbol;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInto(this, context);
  }

  @Override
  public PlanNode clone() {
    return new IntoNode(id, null, database, table, columns, neededInputColumnNames, rowCountSymbol);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ImmutableList.of(rowCountSymbol.getName());
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.of(rowCountSymbol);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INTO.serialize(byteBuffer);
    ReadWriteIOUtils.write(database, byteBuffer);
    ReadWriteIOUtils.write(table, byteBuffer);
    Symbol.serialize(rowCountSymbol, byteBuffer);
    ReadWriteIOUtils.write(columns.size(), byteBuffer);
    for (ColumnSchema tableColumn : columns) {
      ColumnSchema.serialize(tableColumn, byteBuffer);
    }
    for (Symbol column : neededInputColumnNames) {
      Symbol.serialize(column, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_INTO_NODE.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(table, stream);
    Symbol.serialize(rowCountSymbol, stream);
    ReadWriteIOUtils.write(columns.size(), stream);
    for (ColumnSchema tableColumn : columns) {
      ColumnSchema.serialize(tableColumn, stream);
    }
    for (Symbol column : neededInputColumnNames) {
      Symbol.serialize(column, stream);
    }
  }

  public static IntoNode deserialize(ByteBuffer byteBuffer) {
    String database = ReadWriteIOUtils.readString(byteBuffer);
    String table = ReadWriteIOUtils.readString(byteBuffer);
    Symbol rowCountSymbol = Symbol.deserialize(byteBuffer);
    int columnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<ColumnSchema> columns = new ArrayList<>(columnSize);
    for (int i = 0; i < columnSize; i++) {
      columns.add(ColumnSchema.deserialize(byteBuffer));
    }
    List<Symbol> neededInputColumnNames = new ArrayList<>(columnSize);
    for (int i = 0; i < columnSize; i++) {
      neededInputColumnNames.add(Symbol.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new IntoNode(
        planNodeId, null, database, table, columns, neededInputColumnNames, rowCountSymbol);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new IntoNode(
        id,
        Iterables.getOnlyElement(newChildren),
        database,
        table,
        columns,
        neededInputColumnNames,
        rowCountSymbol);
  }

  @Override
  public String toString() {
    return "IntoNode-" + this.getPlanNodeId();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    IntoNode that = (IntoNode) o;
    return database.equals(that.database)
        && table.equals(that.table)
        && rowCountSymbol.equals(that.rowCountSymbol)
        && Objects.deepEquals(columns, that.columns)
        && Objects.deepEquals(neededInputColumnNames, that.neededInputColumnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), database, table, rowCountSymbol, columns, neededInputColumnNames);
  }

  public List<Type> getOutputType() {
    return ImmutableList.of(Insert.ROWS_TYPE);
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public List<ColumnSchema> getColumns() {
    return columns;
  }

  public List<Symbol> getNeededInputColumnNames() {
    return neededInputColumnNames;
  }

  public Symbol getRowCountSymbol() {
    return rowCountSymbol;
  }
}
