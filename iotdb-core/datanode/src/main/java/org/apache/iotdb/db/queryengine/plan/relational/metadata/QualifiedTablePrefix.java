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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import com.google.errorprone.annotations.Immutable;

import java.util.Objects;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.metadata.MetadataUtil.checkDatabaseName;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.MetadataUtil.checkTableName;

@Immutable
public class QualifiedTablePrefix {
  private final String databaseName;
  private final Optional<String> tableName;

  public QualifiedTablePrefix(String databaseName) {
    this.databaseName = checkDatabaseName(databaseName);
    this.tableName = Optional.empty();
  }

  public QualifiedTablePrefix(String databaseName, String tableName) {
    this.databaseName = checkDatabaseName(databaseName);
    this.tableName = Optional.of(checkTableName(tableName));
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public Optional<String> getTableName() {
    return tableName;
  }

  public boolean hasTableName() {
    return tableName.isPresent();
  }

  public Optional<QualifiedObjectName> asQualifiedObjectName() {
    return tableName.map(s -> new QualifiedObjectName(databaseName, s));
  }

  public boolean matches(QualifiedObjectName objectName) {
    return Objects.equals(databaseName, objectName.getDatabaseName())
        && tableName.map(table -> Objects.equals(table, objectName.getObjectName())).orElse(true);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    QualifiedTablePrefix o = (QualifiedTablePrefix) obj;
    return Objects.equals(databaseName, o.databaseName) && Objects.equals(tableName, o.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, tableName);
  }

  @Override
  public String toString() {
    return databaseName + '.' + tableName.orElse("*");
  }
}
