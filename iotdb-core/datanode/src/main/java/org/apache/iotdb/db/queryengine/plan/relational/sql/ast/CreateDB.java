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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateDB extends Statement {

  private final boolean ifNotExists;
  private final String dbName;
  private final List<Property> properties;

  public CreateDB(final boolean ifNotExists, final String dbName) {
    super(null);
    this.ifNotExists = ifNotExists;
    this.dbName = requireNonNull(dbName, "dbName is null").toLowerCase(Locale.ENGLISH);
    this.properties = null;
  }

  public CreateDB(final NodeLocation location, final boolean ifNotExists, final String dbName) {
    super(requireNonNull(location, "location is null"));
    this.ifNotExists = ifNotExists;
    this.dbName = requireNonNull(dbName, "dbName is null").toLowerCase(Locale.ENGLISH);
    this.properties = null;
  }

  public CreateDB(final boolean ifNotExists, final String dbName, final List<Property> properties) {
    super(null);
    this.ifNotExists = ifNotExists;
    this.dbName = requireNonNull(dbName, "dbName is null").toLowerCase(Locale.ENGLISH);
    this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
  }

  public CreateDB(
      final NodeLocation location,
      final boolean ifNotExists,
      final String dbName,
      final List<Property> properties) {
    super(requireNonNull(location, "location is null"));
    this.ifNotExists = ifNotExists;
    this.dbName = requireNonNull(dbName, "dbName is null").toLowerCase(Locale.ENGLISH);
    this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
  }

  public String getDbName() {
    return dbName;
  }

  public boolean isSetIfNotExists() {
    return ifNotExists;
  }

  public List<Property> getProperties() {
    return properties;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateDB(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.copyOf(properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbName, ifNotExists, properties);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final CreateDB o = (CreateDB) obj;
    return Objects.equals(dbName, o.dbName)
        && Objects.equals(ifNotExists, o.ifNotExists)
        && Objects.equals(properties, o.properties);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("dbName", dbName)
        .add("ifNotExists", ifNotExists)
        .add("properties", properties)
        .toString();
  }
}
