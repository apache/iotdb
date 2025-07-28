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

import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateDB extends DatabaseStatement {

  public CreateDB(
      final NodeLocation location,
      final boolean exists,
      final String dbName,
      final List<Property> properties) {
    super(requireNonNull(location, "location is null"), exists, dbName, properties);
  }

  @Override
  public DatabaseSchemaStatement.DatabaseSchemaStatementType getType() {
    return DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateDB(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("dbName", dbName)
        .add("ifNotExists", exists)
        .add("properties", properties)
        .toString();
  }
}
