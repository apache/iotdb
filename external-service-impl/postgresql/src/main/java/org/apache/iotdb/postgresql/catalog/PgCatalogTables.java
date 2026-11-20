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

import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The catalog tables registered in IoTDB's {@code information_schema}: PostgreSQL 14 column names,
 * types mapped to the table model ({@code oid} and {@code regproc} → INT64, {@code name}/{@code
 * char}/{@code text} and arrays → STRING, {@code bool} → BOOLEAN, {@code int2}/{@code int4} →
 * INT32, {@code float4} → FLOAT).
 *
 * <p>Only definitions, no other work: the core loads this class while it initialises its
 * information schema, and a failure there would stop the database from starting.
 */
public final class PgCatalogTables {

  /** A column: name and table-model type. */
  public static final class Column {
    private final String name;
    private final TSDataType type;
    private final boolean array;

    Column(String name, TSDataType type, boolean array) {
      this.name = name;
      this.type = type;
      this.array = array;
    }

    /**
     * True for PostgreSQL array columns ({@code aclitem[]}, {@code oid[]}, {@code int2vector},
     * ...): STRING here and always NULL, because the table model has no arrays.
     */
    public boolean array() {
      return array;
    }

    public String name() {
      return name;
    }

    public TSDataType type() {
      return type;
    }
  }

  private static final TSDataType OID = TSDataType.INT64;
  private static final TSDataType REGPROC = TSDataType.INT64;
  private static final TSDataType NAME = TSDataType.STRING;
  private static final TSDataType TEXT = TSDataType.STRING;
  private static final TSDataType CHAR = TSDataType.STRING;
  private static final TSDataType BOOL = TSDataType.BOOLEAN;
  private static final TSDataType INT2 = TSDataType.INT32;
  private static final TSDataType INT4 = TSDataType.INT32;
  private static final TSDataType FLOAT4 = TSDataType.FLOAT;

  /** Prefix of PostgreSQL's own {@code information_schema} views. */
  public static final String INFORMATION_SCHEMA_PREFIX = "pg_is_";

  private static final Map<String, List<Column>> TABLES = new LinkedHashMap<>();

  static {
    table("pg_namespace").col("oid", OID).col("nspname", NAME).col("nspowner", OID).array("nspacl");
    table("pg_class")
        .col("oid", OID)
        .col("relname", NAME)
        .col("relnamespace", OID)
        .col("reltype", OID)
        .col("reloftype", OID)
        .col("relowner", OID)
        .col("relam", OID)
        .col("relfilenode", OID)
        .col("reltablespace", OID)
        .col("relpages", INT4)
        .col("reltuples", FLOAT4)
        .col("relallvisible", INT4)
        .col("reltoastrelid", OID)
        .col("relhasindex", BOOL)
        .col("relisshared", BOOL)
        .col("relpersistence", CHAR)
        .col("relkind", CHAR)
        .col("relnatts", INT2)
        .col("relchecks", INT2)
        .col("relhasrules", BOOL)
        .col("relhastriggers", BOOL)
        .col("relhassubclass", BOOL)
        .col("relrowsecurity", BOOL)
        .col("relforcerowsecurity", BOOL)
        .col("relispopulated", BOOL)
        .col("relreplident", CHAR)
        .col("relispartition", BOOL)
        .col("relrewrite", OID)
        .array("relacl")
        .array("reloptions")
        .col("relpartbound", TEXT);
    table("pg_attribute")
        .col("attrelid", OID)
        .col("attname", NAME)
        .col("atttypid", OID)
        .col("attstattarget", INT4)
        .col("attlen", INT2)
        .col("attnum", INT2)
        .col("attndims", INT4)
        .col("attcacheoff", INT4)
        .col("atttypmod", INT4)
        .col("attbyval", BOOL)
        .col("attalign", CHAR)
        .col("attstorage", CHAR)
        .col("attcompression", CHAR)
        .col("attnotnull", BOOL)
        .col("atthasdef", BOOL)
        .col("atthasmissing", BOOL)
        .col("attidentity", CHAR)
        .col("attgenerated", CHAR)
        .col("attisdropped", BOOL)
        .col("attislocal", BOOL)
        .col("attinhcount", INT4)
        .col("attcollation", OID)
        .array("attacl")
        .array("attoptions")
        .array("attfdwoptions");
    table("pg_type")
        .col("oid", OID)
        .col("typname", NAME)
        .col("typnamespace", OID)
        .col("typowner", OID)
        .col("typlen", INT2)
        .col("typbyval", BOOL)
        .col("typtype", CHAR)
        .col("typcategory", CHAR)
        .col("typispreferred", BOOL)
        .col("typisdefined", BOOL)
        .col("typdelim", CHAR)
        .col("typrelid", OID)
        .col("typsubscript", REGPROC)
        .col("typelem", OID)
        .col("typarray", OID)
        .col("typinput", REGPROC)
        .col("typoutput", REGPROC)
        .col("typreceive", REGPROC)
        .col("typsend", REGPROC)
        .col("typmodin", REGPROC)
        .col("typmodout", REGPROC)
        .col("typanalyze", REGPROC)
        .col("typalign", CHAR)
        .col("typstorage", CHAR)
        .col("typnotnull", BOOL)
        .col("typbasetype", OID)
        .col("typtypmod", INT4)
        .col("typndims", INT4)
        .col("typcollation", OID)
        .col("typdefaultbin", TEXT)
        .col("typdefault", TEXT)
        .array("typacl");
    table("pg_proc")
        .col("oid", OID)
        .col("proname", NAME)
        .col("pronamespace", OID)
        .col("proowner", OID)
        .col("prolang", OID)
        .col("procost", FLOAT4)
        .col("prorows", FLOAT4)
        .col("provariadic", OID)
        .col("prosupport", REGPROC)
        .col("prokind", CHAR)
        .col("prosecdef", BOOL)
        .col("proleakproof", BOOL)
        .col("proisstrict", BOOL)
        .col("proretset", BOOL)
        .col("provolatile", CHAR)
        .col("proparallel", CHAR)
        .col("pronargs", INT2)
        .col("pronargdefaults", INT2)
        .col("prorettype", OID)
        .array("proargtypes")
        .array("proallargtypes")
        .array("proargmodes")
        .array("proargnames")
        .col("proargdefaults", TEXT)
        .array("protrftypes")
        .col("prosrc", TEXT)
        .col("probin", TEXT)
        .col("prosqlbody", TEXT)
        .array("proconfig")
        .array("proacl");
    table("pg_description")
        .col("objoid", OID)
        .col("classoid", OID)
        .col("objsubid", INT4)
        .col("description", TEXT);
    table("pg_range")
        .col("rngtypid", OID)
        .col("rngsubtype", OID)
        .col("rngmultitypid", OID)
        .col("rngcollation", OID)
        .col("rngsubopc", OID)
        .col("rngcanonical", REGPROC)
        .col("rngsubdiff", REGPROC);
    table("pg_enum")
        .col("oid", OID)
        .col("enumtypid", OID)
        .col("enumsortorder", FLOAT4)
        .col("enumlabel", NAME);
    table("pg_index")
        .col("indexrelid", OID)
        .col("indrelid", OID)
        .col("indnatts", INT2)
        .col("indnkeyatts", INT2)
        .col("indisunique", BOOL)
        .col("indisprimary", BOOL)
        .col("indisexclusion", BOOL)
        .col("indimmediate", BOOL)
        .col("indisclustered", BOOL)
        .col("indisvalid", BOOL)
        .col("indcheckxmin", BOOL)
        .col("indisready", BOOL)
        .col("indislive", BOOL)
        .col("indisreplident", BOOL)
        .array("indkey")
        .array("indcollation")
        .array("indclass")
        .array("indoption")
        .col("indexprs", TEXT)
        .col("indpred", TEXT);
    table("pg_constraint")
        .col("oid", OID)
        .col("conname", NAME)
        .col("connamespace", OID)
        .col("contype", CHAR)
        .col("condeferrable", BOOL)
        .col("condeferred", BOOL)
        .col("convalidated", BOOL)
        .col("conrelid", OID)
        .col("contypid", OID)
        .col("conindid", OID)
        .col("conparentid", OID)
        .col("confrelid", OID)
        .col("confupdtype", CHAR)
        .col("confdeltype", CHAR)
        .col("confmatchtype", CHAR)
        .col("conislocal", BOOL)
        .col("coninhcount", INT4)
        .col("connoinherit", BOOL)
        .array("conkey")
        .array("confkey")
        .array("conpfeqop")
        .array("conppeqop")
        .array("conffeqop")
        .array("conexclop")
        .col("conbin", TEXT);
    table("pg_inherits")
        .col("inhrelid", OID)
        .col("inhparent", OID)
        .col("inhseqno", INT4)
        .col("inhdetachpending", BOOL);
    table("pg_attrdef").col("oid", OID).col("adrelid", OID).col("adnum", INT2).col("adbin", TEXT);
    table("pg_am")
        .col("oid", OID)
        .col("amname", NAME)
        .col("amhandler", REGPROC)
        .col("amtype", CHAR);
    table("pg_policy")
        .col("oid", OID)
        .col("polname", NAME)
        .col("polrelid", OID)
        .col("polcmd", CHAR)
        .col("polpermissive", BOOL)
        .array("polroles")
        .col("polqual", TEXT)
        .col("polwithcheck", TEXT);
    table("pg_statistic_ext")
        .col("oid", OID)
        .col("stxrelid", OID)
        .col("stxname", NAME)
        .col("stxnamespace", OID)
        .col("stxowner", OID)
        .col("stxstattarget", INT4)
        .array("stxkeys")
        .array("stxkind")
        .col("stxexprs", TEXT);
    table("pg_publication")
        .col("oid", OID)
        .col("pubname", NAME)
        .col("pubowner", OID)
        .col("puballtables", BOOL)
        .col("pubinsert", BOOL)
        .col("pubupdate", BOOL)
        .col("pubdelete", BOOL)
        .col("pubtruncate", BOOL)
        .col("pubviaroot", BOOL);
    table("pg_publication_rel").col("oid", OID).col("prpubid", OID).col("prrelid", OID);
    table("pg_collation")
        .col("oid", OID)
        .col("collname", NAME)
        .col("collnamespace", OID)
        .col("collowner", OID)
        .col("collprovider", CHAR)
        .col("collisdeterministic", BOOL)
        .col("collencoding", INT4)
        .col("collcollate", NAME)
        .col("collctype", NAME)
        .col("collversion", TEXT);
    table("pg_database")
        .col("oid", OID)
        .col("datname", NAME)
        .col("datdba", OID)
        .col("encoding", INT4)
        .col("datcollate", NAME)
        .col("datctype", NAME)
        .col("datistemplate", BOOL)
        .col("datallowconn", BOOL)
        .col("datconnlimit", INT4)
        .col("datlastsysoid", OID)
        .col("dattablespace", OID)
        .array("datacl");
    table("pg_roles")
        .col("oid", OID)
        .col("rolname", NAME)
        .col("rolsuper", BOOL)
        .col("rolinherit", BOOL)
        .col("rolcreaterole", BOOL)
        .col("rolcreatedb", BOOL)
        .col("rolcanlogin", BOOL)
        .col("rolreplication", BOOL)
        .col("rolconnlimit", INT4)
        .col("rolpassword", TEXT)
        .col("rolvaliduntil", TEXT)
        .col("rolbypassrls", BOOL)
        .array("rolconfig");
    table("pg_user")
        .col("usename", NAME)
        .col("usesysid", OID)
        .col("usecreatedb", BOOL)
        .col("usesuper", BOOL)
        .col("userepl", BOOL)
        .col("usebypassrls", BOOL)
        .col("passwd", TEXT)
        .col("valuntil", TEXT)
        .array("useconfig");
    table("pg_settings")
        .col("name", TEXT)
        .col("setting", TEXT)
        .col("unit", TEXT)
        .col("category", TEXT)
        .col("short_desc", TEXT)
        .col("extra_desc", TEXT)
        .col("context", TEXT)
        .col("vartype", TEXT)
        .col("source", TEXT)
        .col("min_val", TEXT)
        .col("max_val", TEXT)
        .array("enumvals")
        .col("boot_val", TEXT)
        .col("reset_val", TEXT)
        .col("sourcefile", TEXT)
        .col("sourceline", INT4)
        .col("pending_restart", BOOL);

    // PostgreSQL's information_schema views; sql_identifier, character_data and yes_or_no are
    // text, cardinal_number is int4
    table("pg_is_schemata")
        .col("catalog_name", TEXT)
        .col("schema_name", TEXT)
        .col("schema_owner", TEXT)
        .col("default_character_set_catalog", TEXT)
        .col("default_character_set_schema", TEXT)
        .col("default_character_set_name", TEXT)
        .col("sql_path", TEXT);
    table("pg_is_tables")
        .col("table_catalog", TEXT)
        .col("table_schema", TEXT)
        .col("table_name", TEXT)
        .col("table_type", TEXT)
        .col("self_referencing_column_name", TEXT)
        .col("reference_generation", TEXT)
        .col("user_defined_type_catalog", TEXT)
        .col("user_defined_type_schema", TEXT)
        .col("user_defined_type_name", TEXT)
        .col("is_insertable_into", TEXT)
        .col("is_typed", TEXT)
        .col("commit_action", TEXT);
    table("pg_is_columns")
        .col("table_catalog", TEXT)
        .col("table_schema", TEXT)
        .col("table_name", TEXT)
        .col("column_name", TEXT)
        .col("ordinal_position", INT4)
        .col("column_default", TEXT)
        .col("is_nullable", TEXT)
        .col("data_type", TEXT)
        .col("character_maximum_length", INT4)
        .col("character_octet_length", INT4)
        .col("numeric_precision", INT4)
        .col("numeric_precision_radix", INT4)
        .col("numeric_scale", INT4)
        .col("datetime_precision", INT4)
        .col("interval_type", TEXT)
        .col("interval_precision", INT4)
        .col("character_set_catalog", TEXT)
        .col("character_set_schema", TEXT)
        .col("character_set_name", TEXT)
        .col("collation_catalog", TEXT)
        .col("collation_schema", TEXT)
        .col("collation_name", TEXT)
        .col("domain_catalog", TEXT)
        .col("domain_schema", TEXT)
        .col("domain_name", TEXT)
        .col("udt_catalog", TEXT)
        .col("udt_schema", TEXT)
        .col("udt_name", TEXT)
        .col("scope_catalog", TEXT)
        .col("scope_schema", TEXT)
        .col("scope_name", TEXT)
        .col("maximum_cardinality", INT4)
        .col("dtd_identifier", TEXT)
        .col("is_self_referencing", TEXT)
        .col("is_identity", TEXT)
        .col("identity_generation", TEXT)
        .col("identity_start", TEXT)
        .col("identity_increment", TEXT)
        .col("identity_maximum", TEXT)
        .col("identity_minimum", TEXT)
        .col("identity_cycle", TEXT)
        .col("is_generated", TEXT)
        .col("generation_expression", TEXT)
        .col("is_updatable", TEXT);
    table("pg_is_views")
        .col("table_catalog", TEXT)
        .col("table_schema", TEXT)
        .col("table_name", TEXT)
        .col("view_definition", TEXT)
        .col("check_option", TEXT)
        .col("is_updatable", TEXT)
        .col("is_insertable_into", TEXT)
        .col("is_trigger_updatable", TEXT)
        .col("is_trigger_deletable", TEXT)
        .col("is_trigger_insertable_into", TEXT);
    table("pg_is_table_constraints")
        .col("constraint_catalog", TEXT)
        .col("constraint_schema", TEXT)
        .col("constraint_name", TEXT)
        .col("table_catalog", TEXT)
        .col("table_schema", TEXT)
        .col("table_name", TEXT)
        .col("constraint_type", TEXT)
        .col("is_deferrable", TEXT)
        .col("initially_deferred", TEXT)
        .col("enforced", TEXT);
    table("pg_is_key_column_usage")
        .col("constraint_catalog", TEXT)
        .col("constraint_schema", TEXT)
        .col("constraint_name", TEXT)
        .col("table_catalog", TEXT)
        .col("table_schema", TEXT)
        .col("table_name", TEXT)
        .col("column_name", TEXT)
        .col("ordinal_position", INT4)
        .col("position_in_unique_constraint", INT4);
    table("pg_is_referential_constraints")
        .col("constraint_catalog", TEXT)
        .col("constraint_schema", TEXT)
        .col("constraint_name", TEXT)
        .col("unique_constraint_catalog", TEXT)
        .col("unique_constraint_schema", TEXT)
        .col("unique_constraint_name", TEXT)
        .col("match_option", TEXT)
        .col("update_rule", TEXT)
        .col("delete_rule", TEXT);
  }

  /**
   * Tables that never have rows: the table model has no ranges, enums, indexes, constraints,
   * defaults, inheritance, policies, extended statistics or publications. A query over one of them
   * returns no rows whatever it selects (the translator relies on this).
   */
  private static final java.util.Set<String> ALWAYS_EMPTY =
      new java.util.HashSet<>(
          java.util.Arrays.asList(
              "pg_range",
              "pg_enum",
              "pg_index",
              "pg_constraint",
              "pg_inherits",
              "pg_attrdef",
              "pg_policy",
              "pg_statistic_ext",
              "pg_publication",
              "pg_publication_rel",
              "pg_is_table_constraints",
              "pg_is_key_column_usage",
              "pg_is_referential_constraints"));

  private PgCatalogTables() {}

  /** True if {@code table} never has rows. */
  public static boolean alwaysEmpty(String table) {
    return ALWAYS_EMPTY.contains(table);
  }

  /** A column of a catalog table, or {@code null}. */
  public static Column column(String table, String column) {
    List<Column> columns = TABLES.get(table);
    if (columns != null) {
      for (Column c : columns) {
        if (c.name().equals(column)) {
          return c;
        }
      }
    }
    return null;
  }

  /** Table name → columns in order. */
  public static Map<String, List<Column>> tables() {
    return Collections.unmodifiableMap(TABLES);
  }

  /** True if {@code table} is one of the catalog tables. */
  public static boolean contains(String table) {
    return TABLES.containsKey(table);
  }

  /** The PostgreSQL name of a catalog table ({@code pg_is_tables} → {@code tables}). */
  public static String postgresName(String table) {
    return table.startsWith(INFORMATION_SCHEMA_PREFIX)
        ? table.substring(INFORMATION_SCHEMA_PREFIX.length())
        : table;
  }

  private static Builder table(String name) {
    List<Column> columns = new ArrayList<>();
    TABLES.put(name, columns);
    return new Builder(columns);
  }

  private static final class Builder {
    private final List<Column> columns;

    Builder(List<Column> columns) {
      this.columns = columns;
    }

    Builder col(String name, TSDataType type) {
      columns.add(new Column(name, type, false));
      return this;
    }

    Builder array(String name) {
      columns.add(new Column(name, TSDataType.STRING, true));
      return this;
    }
  }
}
