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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MetadataUtil {
  private MetadataUtil() {}

  public static void checkTableName(String databaseName, Optional<String> tableName) {
    checkDatabaseName(databaseName);
    tableName.ifPresent(name -> checkLowerCase(name, "tableName"));
  }

  public static String checkDatabaseName(String databaseName) {
    return checkLowerCase(databaseName, "databaseName");
  }

  public static String checkTableName(String tableName) {
    return checkLowerCase(tableName, "tableName");
  }

  public static void checkObjectName(String dbName, String objectName) {
    checkLowerCase(dbName, "dbName");
    checkLowerCase(objectName, "objectName");
  }

  public static String checkLowerCase(String value, String name) {
    if (value == null) {
      throw new NullPointerException(format("%s is null", name));
    }
    checkArgument(value.equals(value.toLowerCase(ENGLISH)), "%s is not lowercase: %s", name, value);
    return value;
  }

  public static QualifiedObjectName createQualifiedObjectName(
      final SessionInfo session, final QualifiedName name) {
    requireNonNull(session, "session is null");
    requireNonNull(name, "name is null");
    if (name.getParts().size() > 2) {
      throw new SemanticException(String.format("Too many dots in table name: %s", name));
    }

    final List<String> parts = Lists.reverse(name.getParts());
    final String objectName = parts.get(0);
    final String databaseName =
        (parts.size() > 1)
            ? parts.get(1)
            : session
                .getDatabaseName()
                .orElseThrow(
                    () ->
                        new SemanticException(
                            "Database must be specified when session database is not set"));

    return new QualifiedObjectName(databaseName, objectName);
  }

  public static boolean tableExists(Metadata metadata, SessionInfo session, String table) {
    if (!session.getDatabaseName().isPresent()) {
      return false;
    }
    QualifiedObjectName name = new QualifiedObjectName(session.getDatabaseName().get(), table);
    return metadata.tableExists(name);
  }

  public static class TableMetadataBuilder {
    public static TableMetadataBuilder tableMetadataBuilder(String tableName) {
      return new TableMetadataBuilder(tableName);
    }

    private final String tableName;
    private final ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
    private final ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
    private final Optional<String> comment;

    private TableMetadataBuilder(String tableName) {
      this(tableName, Optional.empty());
    }

    private TableMetadataBuilder(String tableName, Optional<String> comment) {
      this.tableName = tableName;
      this.comment = comment;
    }

    public TableMetadataBuilder column(String columnName, Type type) {
      columns.add(new ColumnMetadata(columnName, type));
      return this;
    }

    public TableMetadataBuilder hiddenColumn(String columnName, Type type) {
      columns.add(
          ColumnMetadata.builder().setName(columnName).setType(type).setHidden(true).build());
      return this;
    }

    public TableMetadataBuilder property(String name, Object value) {
      properties.put(name, value);
      return this;
    }

    public TableMetadata build() {
      return new TableMetadata(tableName, columns.build(), properties.buildOrThrow(), comment);
    }
  }
}
