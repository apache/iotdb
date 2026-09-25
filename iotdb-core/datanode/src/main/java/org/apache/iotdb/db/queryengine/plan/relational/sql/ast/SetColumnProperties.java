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

import org.apache.iotdb.commons.i18n.LBACMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AstMemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SetColumnProperties extends Statement {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SetColumnProperties.class);

  private final QualifiedName tableName;
  private final Identifier columnName;
  private final List<Property> properties;
  private final boolean tableIfExists;
  private final boolean columnIfExists;

  public SetColumnProperties(
      final NodeLocation location,
      final QualifiedName tableName,
      final Identifier columnName,
      final List<Property> properties,
      final boolean tableIfExists,
      final boolean columnIfExists) {
    super(requireNonNull(location, LBACMessages.EXCEPTION_LOCATION_IS_NULL_399F8D73));
    this.tableName = requireNonNull(tableName, LBACMessages.EXCEPTION_TABLENAME_IS_NULL_6B6687B9);
    this.columnName =
        requireNonNull(columnName, LBACMessages.EXCEPTION_COLUMNNAME_IS_NULL_46BD2848);
    this.properties =
        ImmutableList.copyOf(
            requireNonNull(properties, LBACMessages.EXCEPTION_PROPERTIES_IS_NULL_08E70FBB));
    this.tableIfExists = tableIfExists;
    this.columnIfExists = columnIfExists;
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public Identifier getColumnName() {
    return columnName;
  }

  public List<Property> getProperties() {
    return properties;
  }

  public boolean tableIfExists() {
    return tableIfExists;
  }

  public boolean columnIfExists() {
    return columnIfExists;
  }

  @Override
  public <R, C> R accept(final IAstVisitor<R, C> visitor, final C context) {
    return ((AstVisitor<R, C>) visitor).visitSetColumnProperties(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SetColumnProperties that = (SetColumnProperties) o;
    return tableIfExists == that.tableIfExists
        && columnIfExists == that.columnIfExists
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(columnName, that.columnName)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, columnName, properties, tableIfExists, columnIfExists);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", tableName)
        .add("columnName", columnName)
        .add("properties", properties)
        .add("tableIfExists", tableIfExists)
        .add("columnIfExists", columnIfExists)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += tableName.ramBytesUsed();
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(columnName);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(properties);
    return size;
  }
}
