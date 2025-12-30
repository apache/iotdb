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

import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class AlterColumnDataType extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlterColumnDataType.class);
  private final QualifiedName tableName;
  private final Identifier columnName;
  private final DataType dataType;
  private final boolean ifTableExists;
  private final boolean ifColumnExists;
  private final boolean view;

  public AlterColumnDataType(
      @Nullable NodeLocation location,
      QualifiedName tableName,
      Identifier columnName,
      DataType dataType,
      boolean ifTableExists,
      boolean ifColumnExists,
      boolean view) {
    super(location);
    this.tableName = tableName;
    this.columnName = columnName;
    this.dataType = dataType;
    this.ifTableExists = ifTableExists;
    this.ifColumnExists = ifColumnExists;
    this.view = view;
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlterColumnDataType that = (AlterColumnDataType) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(columnName, that.columnName)
        && Objects.equals(dataType, that.dataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, columnName, dataType);
  }

  @Override
  public String toString() {
    return "AlterColumnDataType{"
        + "tableName="
        + tableName
        + ", columnName="
        + columnName
        + ", dataType="
        + dataType
        + ", view="
        + view
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAlterColumnDataType(this, context);
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public Identifier getColumnName() {
    return columnName;
  }

  public DataType getDataType() {
    return dataType;
  }

  public boolean isIfTableExists() {
    return ifTableExists;
  }

  public boolean isIfColumnExists() {
    return ifColumnExists;
  }

  public boolean isView() {
    return view;
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += tableName == null ? 0L : tableName.ramBytesUsed();
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(columnName);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(dataType);
    return size;
  }
}
