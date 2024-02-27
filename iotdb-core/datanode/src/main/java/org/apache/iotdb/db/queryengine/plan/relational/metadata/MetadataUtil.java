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

import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.QualifiedName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

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
    checkLowerCase(dbName, "schemaName");
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
      Session session, Node node, QualifiedName name) {
    requireNonNull(session, "session is null");
    requireNonNull(name, "name is null");
    if (name.getParts().size() > 3) {
      throw new TrinoException(SYNTAX_ERROR, format("Too many dots in table name: %s", name));
    }

    List<String> parts = Lists.reverse(name.getParts());
    String objectName = parts.get(0);
    String schemaName =
        (parts.size() > 1)
            ? parts.get(1)
            : session
                .getSchema()
                .orElseThrow(
                    () ->
                        semanticException(
                            MISSING_SCHEMA_NAME,
                            node,
                            "Schema must be specified when session schema is not set"));
    String catalogName =
        (parts.size() > 2)
            ? parts.get(2)
            : session
                .getCatalog()
                .orElseThrow(
                    () ->
                        semanticException(
                            MISSING_CATALOG_NAME,
                            node,
                            "Catalog must be specified when session catalog is not set"));

    return new QualifiedObjectName(catalogName, schemaName, objectName);
  }

  public static boolean tableExists(Metadata metadata, Session session, String table) {
    if (session.getCatalog().isEmpty() || session.getSchema().isEmpty()) {
      return false;
    }
    QualifiedObjectName name =
        new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), table);
    return metadata.getTableHandle(session, name).isPresent();
  }

  public static void checkRoleExists(
      Session session,
      Node node,
      Metadata metadata,
      TrinoPrincipal principal,
      Optional<String> catalog) {
    if (principal.getType() == ROLE) {
      checkRoleExists(session, node, metadata, principal.getName(), catalog);
    }
  }

  public static void checkRoleExists(
      Session session, Node node, Metadata metadata, String role, Optional<String> catalog) {
    if (!metadata.roleExists(session, role, catalog)) {
      throw semanticException(
          ROLE_NOT_FOUND,
          node,
          "Role '%s' does not exist%s",
          role,
          catalog.map(c -> format(" in catalog '%s'", c)).orElse(""));
    }
  }

  public static Optional<String> processRoleCommandCatalog(
      Metadata metadata, Session session, Node node, Optional<String> catalog) {
    boolean legacyCatalogRoles = isLegacyCatalogRoles(session);
    // old role commands use only supported catalog roles and used session catalog as the default
    if (catalog.isEmpty() && legacyCatalogRoles) {
      catalog = session.getCatalog();
      if (catalog.isEmpty()) {
        throw semanticException(MISSING_CATALOG_NAME, node, "Session catalog must be set");
      }
    }
    catalog.ifPresent(
        catalogName -> getRequiredCatalogHandle(metadata, session, node, catalogName));

    if (catalog.isPresent() && !metadata.isCatalogManagedSecurity(session, catalog.get())) {
      throw semanticException(
          NOT_SUPPORTED, node, "Catalog '%s' does not support role management", catalog.get());
    }

    return catalog;
  }

  public static class TableMetadataBuilder {
    public static TableMetadataBuilder tableMetadataBuilder(SchemaTableName tableName) {
      return new TableMetadataBuilder(tableName);
    }

    private final SchemaTableName tableName;
    private final ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
    private final ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
    private final Optional<String> comment;

    private TableMetadataBuilder(SchemaTableName tableName) {
      this(tableName, Optional.empty());
    }

    private TableMetadataBuilder(SchemaTableName tableName, Optional<String> comment) {
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

    public ConnectorTableMetadata build() {
      return new ConnectorTableMetadata(
          tableName, columns.build(), properties.buildOrThrow(), comment);
    }
  }
}
