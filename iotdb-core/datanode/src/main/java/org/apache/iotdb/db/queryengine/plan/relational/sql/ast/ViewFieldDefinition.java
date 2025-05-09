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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Objects;

public class ViewFieldDefinition extends ColumnDefinition {

  private final Identifier from;

  public ViewFieldDefinition(
      final NodeLocation location,
      final Identifier name,
      final DataType type,
      final @Nullable String charsetName,
      final @Nullable String comment,
      final @Nullable Identifier from) {
    super(location, name, type, TsTableColumnCategory.FIELD, charsetName, comment);
    this.from = from;
  }

  @Override
  protected DataType getDefaultType(final DataType type) {
    return Objects.nonNull(type)
        ? type
        : new GenericDataType(new Identifier("unknown"), new ArrayList<>());
  }

  public Identifier getFrom() {
    return from;
  }

  public <R, C> R accept(final AstVisitor<R, C> visitor, C context) {
    return visitor.visitViewFieldDefinition(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o) && Objects.equals(from, ((ViewFieldDefinition) o).from);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), from);
  }
}
