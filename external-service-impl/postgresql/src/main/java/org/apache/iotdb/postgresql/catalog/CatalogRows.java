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

package org.apache.iotdb.postgresql.catalog;

import org.apache.iotdb.postgresql.catalog.CatalogSnapshot.Attribute;
import org.apache.iotdb.postgresql.catalog.CatalogSnapshot.Namespace;
import org.apache.iotdb.postgresql.catalog.CatalogSnapshot.Relation;
import org.apache.iotdb.postgresql.types.PgType;

import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The rows of the catalog tables for one query: constant rows from {@link StaticCatalog}, the
 * others from the user's {@link CatalogSnapshot}. A row maps column names to values; columns
 * without a value are NULL.
 */
public final class CatalogRows {

  /**
   * The name of the one database a PostgreSQL client sees ({@code pg_database}, {@code
   * table_catalog}). Clients name a database at connect time, but it does not select data (spec
   * Q2).
   */
  public static final String DATABASE = "iotdb";

  private static final List<Map<String, Object>> TYPES = StaticCatalog.types();
  private static final List<Map<String, Object>> PROCS = StaticCatalog.procs();
  private static final List<Map<String, Object>> ACCESS_METHODS = StaticCatalog.accessMethods();
  private static final List<Map<String, Object>> SETTINGS = StaticCatalog.settings();
  private static final List<Map<String, Object>> COLLATIONS = StaticCatalog.collations();

  private final String user;
  private final Supplier<CatalogSnapshot> snapshotSource;
  private CatalogSnapshot snapshot;

  /**
   * @param user the querying user
   * @param snapshot the user's snapshot, only built for tables that need it
   */
  public CatalogRows(String user, Supplier<CatalogSnapshot> snapshot) {
    this.user = user;
    this.snapshotSource = snapshot;
  }

  /** The rows of a catalog table, empty for tables without content. */
  public List<Map<String, Object>> rows(String table) {
    switch (table) {
      case "pg_type":
        return TYPES;
      case "pg_proc":
        return PROCS;
      case "pg_am":
        return ACCESS_METHODS;
      case "pg_settings":
        return SETTINGS;
      case "pg_collation":
        return COLLATIONS;
      case "pg_namespace":
        return namespaces();
      case "pg_class":
        return classes();
      case "pg_attribute":
        return attributes();
      case "pg_description":
        return descriptions();
      case "pg_database":
        return database();
      case "pg_roles":
        return roles();
      case "pg_user":
        return users();
      case "pg_is_schemata":
        return schemata();
      case "pg_is_tables":
        return tables();
      case "pg_is_columns":
        return columns();
      case "pg_is_views":
        return views();
      default:
        // pg_range, pg_enum, pg_index, pg_constraint, pg_inherits, pg_attrdef and the
        // constraint views: the table model has no ranges, enums, indexes or constraints
        return Collections.emptyList();
    }
  }

  private CatalogSnapshot snapshot() {
    if (snapshot == null) {
      snapshot = snapshotSource.get();
    }
    return snapshot;
  }

  /** The OID of a role name, stable for the same name. */
  static long roleOid(String name) {
    return CatalogSnapshot.allocate("u:" + name, new java.util.HashSet<>());
  }

  // ---------------------------------------------------------------------------- pg_catalog

  private List<Map<String, Object>> namespaces() {
    List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(namespace(StaticCatalog.PG_CATALOG, "pg_catalog", StaticCatalog.OWNER));
    rows.add(
        namespace(StaticCatalog.INFORMATION_SCHEMA, "information_schema", StaticCatalog.OWNER));
    for (Namespace namespace : snapshot().namespaces()) {
      rows.add(namespace(namespace.oid(), namespace.name(), StaticCatalog.OWNER));
    }
    return rows;
  }

  private static Map<String, Object> namespace(long oid, String name, long owner) {
    return StaticCatalog.row("oid", oid, "nspname", name, "nspowner", owner);
  }

  private List<Map<String, Object>> classes() {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (StaticCatalog.SystemRelation relation : StaticCatalog.systemRelations()) {
      rows.add(
          pgClass(
              relation.oid,
              relation.name,
              relation.namespace,
              relation.kind,
              PgCatalogTables.tables().get(relation.table).size()));
    }
    for (Relation relation : snapshot().relations()) {
      rows.add(
          pgClass(
              relation.oid(),
              relation.name(),
              relation.namespace().oid(),
              relation.kind(),
              relation.attributes().size()));
    }
    return rows;
  }

  private static Map<String, Object> pgClass(
      long oid, String name, long namespace, char kind, int columns) {
    Map<String, Object> row = new HashMap<>();
    row.put("oid", oid);
    row.put("relname", name);
    row.put("relnamespace", namespace);
    row.put("reltype", 0L);
    row.put("reloftype", 0L);
    row.put("relowner", StaticCatalog.OWNER);
    row.put("relam", kind == 'r' ? 2L : 0L);
    row.put("relfilenode", 0L);
    row.put("reltablespace", 0L);
    row.put("relpages", 0);
    row.put("reltuples", -1.0f);
    row.put("relallvisible", 0);
    row.put("reltoastrelid", 0L);
    row.put("relhasindex", false);
    row.put("relisshared", false);
    row.put("relpersistence", "p");
    row.put("relkind", String.valueOf(kind));
    row.put("relnatts", columns);
    row.put("relchecks", 0);
    row.put("relhasrules", false);
    row.put("relhastriggers", false);
    row.put("relhassubclass", false);
    row.put("relrowsecurity", false);
    row.put("relforcerowsecurity", false);
    row.put("relispopulated", true);
    row.put("relreplident", kind == 'r' ? "d" : "n");
    row.put("relispartition", false);
    row.put("relrewrite", 0L);
    return row;
  }

  private List<Map<String, Object>> attributes() {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (StaticCatalog.SystemRelation relation : StaticCatalog.systemRelations()) {
      int number = 0;
      for (PgCatalogTables.Column column : PgCatalogTables.tables().get(relation.table)) {
        number++;
        PgType type = PgType.of(column.type()).orElse(PgType.TEXT);
        rows.add(pgAttribute(relation.oid, column.name(), number, type, false));
      }
    }
    for (Relation relation : snapshot().relations()) {
      for (Attribute attribute : relation.attributes()) {
        rows.add(
            pgAttribute(
                relation.oid(),
                attribute.name(),
                attribute.number(),
                attribute.type(),
                attribute.notNull()));
      }
    }
    return rows;
  }

  private static Map<String, Object> pgAttribute(
      long relation, String name, int number, PgType type, boolean notNull) {
    Optional<StaticCatalog.TypeDef> def = StaticCatalog.type(type.oid());
    Map<String, Object> row = new HashMap<>();
    row.put("attrelid", relation);
    row.put("attname", name);
    row.put("atttypid", (long) type.oid());
    row.put("attstattarget", -1);
    row.put("attlen", (int) type.size());
    row.put("attnum", number);
    row.put("attndims", 0);
    row.put("attcacheoff", -1);
    row.put("atttypmod", -1);
    row.put("attbyval", def.map(StaticCatalog.TypeDef::byval).orElse(false));
    row.put("attalign", String.valueOf(def.map(StaticCatalog.TypeDef::align).orElse('i')));
    row.put("attstorage", String.valueOf(def.map(StaticCatalog.TypeDef::storage).orElse('p')));
    row.put("attcompression", "");
    row.put("attnotnull", notNull);
    row.put("atthasdef", false);
    row.put("atthasmissing", false);
    row.put("attidentity", "");
    row.put("attgenerated", "");
    row.put("attisdropped", false);
    row.put("attislocal", true);
    row.put("attinhcount", 0);
    row.put("attcollation", def.map(StaticCatalog.TypeDef::collation).orElse(0L));
    return row;
  }

  private List<Map<String, Object>> descriptions() {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Relation relation : snapshot().relations()) {
      if (relation.description() != null) {
        rows.add(description(relation.oid(), 0, relation.description()));
      }
      for (Attribute attribute : relation.attributes()) {
        rows.add(description(relation.oid(), attribute.number(), attribute.description()));
      }
    }
    return rows;
  }

  private static Map<String, Object> description(long object, int sub, String text) {
    return StaticCatalog.row(
        "objoid", object, "classoid", StaticCatalog.PG_CLASS, "objsubid", sub, "description", text);
  }

  private static List<Map<String, Object>> database() {
    Map<String, Object> row = new HashMap<>();
    row.put("oid", CatalogSnapshot.FIRST_USER_OID - 1);
    row.put("datname", DATABASE);
    row.put("datdba", StaticCatalog.OWNER);
    row.put("encoding", 6); // UTF8
    row.put("datcollate", "C");
    row.put("datctype", "C");
    row.put("datistemplate", false);
    row.put("datallowconn", true);
    row.put("datconnlimit", -1);
    row.put("datlastsysoid", CatalogSnapshot.FIRST_USER_OID - 1);
    row.put("dattablespace", 1663L);
    return Collections.singletonList(row);
  }

  private List<Map<String, Object>> roles() {
    Map<String, Object> row = new HashMap<>();
    row.put("oid", roleOid(user));
    row.put("rolname", user);
    row.put("rolsuper", false);
    row.put("rolinherit", true);
    row.put("rolcreaterole", false);
    row.put("rolcreatedb", false);
    row.put("rolcanlogin", true);
    row.put("rolreplication", false);
    row.put("rolconnlimit", -1);
    row.put("rolpassword", "********");
    row.put("rolbypassrls", false);
    return Collections.singletonList(row);
  }

  private List<Map<String, Object>> users() {
    Map<String, Object> row = new HashMap<>();
    row.put("usename", user);
    row.put("usesysid", roleOid(user));
    row.put("usecreatedb", false);
    row.put("usesuper", false);
    row.put("userepl", false);
    row.put("usebypassrls", false);
    row.put("passwd", "********");
    return Collections.singletonList(row);
  }

  // ---------------------------------------------------------------------------- information_schema

  /** A relation of either kind, as the information_schema views list it. */
  private static final class Listed {
    final String schema;
    final String name;
    final char kind;
    final List<Attribute> attributes;

    Listed(String schema, String name, char kind, List<Attribute> attributes) {
      this.schema = schema;
      this.name = name;
      this.kind = kind;
      this.attributes = attributes;
    }
  }

  private List<Listed> listed() {
    List<Listed> all = new ArrayList<>();
    for (StaticCatalog.SystemRelation relation : StaticCatalog.systemRelations()) {
      List<Attribute> attributes = new ArrayList<>();
      for (PgCatalogTables.Column column : PgCatalogTables.tables().get(relation.table)) {
        attributes.add(
            new Attribute(
                column.name(),
                attributes.size() + 1,
                PgType.of(column.type()).orElse(PgType.TEXT),
                false,
                null));
      }
      all.add(
          new Listed(
              relation.namespace == StaticCatalog.PG_CATALOG ? "pg_catalog" : "information_schema",
              relation.name,
              relation.kind,
              attributes));
    }
    for (Relation relation : snapshot().relations()) {
      all.add(
          new Listed(
              relation.namespace().name(),
              relation.name(),
              relation.kind(),
              relation.attributes()));
    }
    return all;
  }

  private List<Map<String, Object>> schemata() {
    List<Map<String, Object>> rows = new ArrayList<>();
    List<String> names = new ArrayList<>();
    names.add("pg_catalog");
    names.add("information_schema");
    for (Namespace namespace : snapshot().namespaces()) {
      names.add(namespace.name());
    }
    for (String name : names) {
      rows.add(
          StaticCatalog.row(
              "catalog_name",
              DATABASE,
              "schema_name",
              name,
              "schema_owner",
              name.equals("pg_catalog") || name.equals("information_schema") ? "postgres" : user));
    }
    return rows;
  }

  private List<Map<String, Object>> tables() {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Listed relation : listed()) {
      Map<String, Object> row = new HashMap<>();
      row.put("table_catalog", DATABASE);
      row.put("table_schema", relation.schema);
      row.put("table_name", relation.name);
      row.put("table_type", relation.kind == 'v' ? "VIEW" : "BASE TABLE");
      row.put("is_insertable_into", "NO");
      row.put("is_typed", "NO");
      rows.add(row);
    }
    return rows;
  }

  private List<Map<String, Object>> views() {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Listed relation : listed()) {
      if (relation.kind != 'v') {
        continue;
      }
      Map<String, Object> row = new HashMap<>();
      row.put("table_catalog", DATABASE);
      row.put("table_schema", relation.schema);
      row.put("table_name", relation.name);
      row.put("check_option", "NONE");
      row.put("is_updatable", "NO");
      row.put("is_insertable_into", "NO");
      row.put("is_trigger_updatable", "NO");
      row.put("is_trigger_deletable", "NO");
      row.put("is_trigger_insertable_into", "NO");
      rows.add(row);
    }
    return rows;
  }

  private List<Map<String, Object>> columns() {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Listed relation : listed()) {
      for (Attribute attribute : relation.attributes) {
        PgType type = attribute.type();
        Map<String, Object> row = new HashMap<>();
        row.put("table_catalog", DATABASE);
        row.put("table_schema", relation.schema);
        row.put("table_name", relation.name);
        row.put("column_name", attribute.name());
        row.put("ordinal_position", attribute.number());
        row.put("is_nullable", attribute.notNull() ? "NO" : "YES");
        row.put("data_type", sqlTypeName(type));
        switch (type) {
          case INT2:
            numeric(row, 16, 2, 0);
            break;
          case INT4:
            numeric(row, 32, 2, 0);
            break;
          case INT8:
            numeric(row, 64, 2, 0);
            break;
          case FLOAT4:
            numeric(row, 24, 2, null);
            break;
          case FLOAT8:
            numeric(row, 53, 2, null);
            break;
          case DATE:
            row.put("datetime_precision", 0);
            break;
          case TIMESTAMP:
          case TIMESTAMPTZ:
            row.put("datetime_precision", 6);
            break;
          case TEXT:
          case VARCHAR:
            row.put("character_octet_length", 1073741824);
            break;
          default:
            break;
        }
        row.put("udt_catalog", DATABASE);
        row.put("udt_schema", "pg_catalog");
        row.put("udt_name", type.typeName());
        row.put("dtd_identifier", String.valueOf(attribute.number()));
        row.put("is_self_referencing", "NO");
        row.put("is_identity", "NO");
        row.put("identity_cycle", "NO");
        row.put("is_generated", "NEVER");
        row.put("is_updatable", "NO");
        rows.add(row);
      }
    }
    return rows;
  }

  private static void numeric(Map<String, Object> row, int precision, int radix, Integer scale) {
    row.put("numeric_precision", precision);
    row.put("numeric_precision_radix", radix);
    row.put("numeric_scale", scale);
  }

  /** The SQL-standard type name that {@code information_schema.columns.data_type} shows. */
  static String sqlTypeName(PgType type) {
    switch (type) {
      case BOOL:
        return "boolean";
      case INT2:
        return "smallint";
      case INT4:
        return "integer";
      case INT8:
        return "bigint";
      case FLOAT4:
        return "real";
      case FLOAT8:
        return "double precision";
      case VARCHAR:
        return "character varying";
      case TIMESTAMP:
        return "timestamp without time zone";
      case TIMESTAMPTZ:
        return "timestamp with time zone";
      default:
        return type.typeName();
    }
  }

  /** Converts a row value to the column's table-model type ({@code null} stays null). */
  static Object convert(Object value, TSDataType type) {
    if (value == null) {
      return null;
    }
    switch (type) {
      case INT64:
        return ((Number) value).longValue();
      case INT32:
        return ((Number) value).intValue();
      case FLOAT:
        return ((Number) value).floatValue();
      case DOUBLE:
        return ((Number) value).doubleValue();
      case BOOLEAN:
        return value;
      default:
        return String.valueOf(value);
    }
  }
}
