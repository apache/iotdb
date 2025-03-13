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

import org.apache.iotdb.commons.path.PartialPath;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class CreateTableView extends CreateTable {
  private final PartialPath prefixPath;
  private final boolean replace;
  private final boolean restrict;

  public CreateTableView(
      final NodeLocation location,
      final QualifiedName name,
      final List<ColumnDefinition> elements,
      final boolean ifNotExists,
      final @Nullable String charsetName,
      final @Nullable String comment,
      final List<Property> properties,
      final PartialPath prefixPath,
      final boolean replace,
      final boolean restrict) {
    super(location, name, elements, ifNotExists, charsetName, comment, properties);
    this.prefixPath = prefixPath;
    this.replace = replace;
    this.restrict = restrict;
  }

  public PartialPath getPrefixPath() {
    return prefixPath;
  }

  public boolean isReplace() {
    return replace;
  }

  public boolean isRestrict() {
    return restrict;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateTableView(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(prefixPath, ((CreateTableView) o).prefixPath)
        && replace == ((CreateTableView) o).replace
        && restrict == ((CreateTableView) o).restrict;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), prefixPath, replace, restrict);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", getName())
        .add("elements", getElements())
        .add("ifNotExists", isIfNotExists())
        .add("charsetName", getCharsetName())
        .add("properties", getProperties())
        .add("prefixPath", prefixPath)
        .add("replace", replace)
        .add("restrict", restrict)
        .toString();
  }
}
