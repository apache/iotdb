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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Constant catalog content: the types of the type mapping and the catalog types, their array types
 * and their I/O functions as in PostgreSQL 14 (OIDs, names, sizes, alignment), access methods and
 * settings. Npgsql 4.0 loads the types at connect time and classifies a type as an array by its
 * receive function {@code array_recv}.
 */
public final class StaticCatalog {

  /** OIDs of the two system namespaces. */
  public static final long PG_CATALOG = 11;

  public static final long INFORMATION_SCHEMA = 13000;

  /** OID of the bootstrap superuser, the owner of every catalog object. */
  public static final long OWNER = 10;

  /** OID of {@code pg_class}, the {@code classoid} of table and column descriptions. */
  public static final long PG_CLASS = 1259;

  /** OID of the default collation, set for text columns. */
  public static final long DEFAULT_COLLATION = 100;

  private static final long ARRAY_IN = 750;
  private static final long ARRAY_OUT = 751;
  private static final long ARRAY_RECV = 2400;
  private static final long ARRAY_SEND = 2401;
  private static final long ARRAY_SUBSCRIPT = 6179;
  private static final long RAW_ARRAY_SUBSCRIPT = 6180;
  private static final long CSTRING = 2275;
  private static final long BYTEA = 17;

  /** A {@code pg_type} row. */
  public static final class TypeDef {
    final long oid;
    final String name;
    final int len;
    final boolean byval;
    final char typtype;
    final char category;
    final boolean preferred;
    final char align;
    final char storage;
    final long elem;
    final long array;
    final long subscript;
    final long[] io;
    final long collation;

    TypeDef(
        long oid,
        String name,
        int len,
        boolean byval,
        char typtype,
        char category,
        boolean preferred,
        char align,
        char storage,
        long elem,
        long array,
        long subscript,
        long[] io,
        long collation) {
      this.oid = oid;
      this.name = name;
      this.len = len;
      this.byval = byval;
      this.typtype = typtype;
      this.category = category;
      this.preferred = preferred;
      this.align = align;
      this.storage = storage;
      this.elem = elem;
      this.array = array;
      this.subscript = subscript;
      this.io = io;
      this.collation = collation;
    }

    public long oid() {
      return oid;
    }

    public String name() {
      return name;
    }

    public int len() {
      return len;
    }

    public boolean byval() {
      return byval;
    }

    public char align() {
      return align;
    }

    public char storage() {
      return storage;
    }

    public long collation() {
      return collation;
    }
  }

  /** A system relation: a catalog table or view. */
  static final class SystemRelation {
    final long oid;
    final String table;
    final long namespace;
    final String name;
    final char kind;

    SystemRelation(long oid, String table, long namespace, String name, char kind) {
      this.oid = oid;
      this.table = table;
      this.namespace = namespace;
      this.name = name;
      this.kind = kind;
    }
  }

  private static final Map<Long, String> PROCS = new LinkedHashMap<>();
  private static final Map<Long, TypeDef> TYPES = new LinkedHashMap<>();
  private static final Map<Long, Long> PROC_RESULT = new HashMap<>();
  private static final List<SystemRelation> RELATIONS = new ArrayList<>();

  static {
    // name, oid, len, byval, category, preferred, align, storage, array, collation,
    // in, out, recv, send, typmodin, typmodout (0 = none) with the function names
    base(
        16,
        "bool",
        1,
        true,
        'B',
        true,
        'c',
        'p',
        1000,
        0,
        f(1242, "boolin"),
        f(1243, "boolout"),
        f(2436, "boolrecv"),
        f(2437, "boolsend"));
    base(
        17,
        "bytea",
        -1,
        false,
        'U',
        false,
        'i',
        'x',
        1001,
        0,
        f(1244, "byteain"),
        f(31, "byteaout"),
        f(2412, "bytearecv"),
        f(2413, "byteasend"));
    base(
        18,
        "char",
        1,
        true,
        'Z',
        false,
        'c',
        'p',
        1002,
        0,
        f(1245, "charin"),
        f(33, "charout"),
        f(2434, "charrecv"),
        f(2435, "charsend"));
    base(
        19,
        "name",
        64,
        false,
        'S',
        false,
        'c',
        'p',
        1003,
        950,
        f(34, "namein"),
        f(35, "nameout"),
        f(2422, "namerecv"),
        f(2423, "namesend"));
    base(
        20,
        "int8",
        8,
        true,
        'N',
        false,
        'd',
        'p',
        1016,
        0,
        f(460, "int8in"),
        f(461, "int8out"),
        f(2408, "int8recv"),
        f(2409, "int8send"));
    base(
        21,
        "int2",
        2,
        true,
        'N',
        false,
        's',
        'p',
        1005,
        0,
        f(38, "int2in"),
        f(39, "int2out"),
        f(2404, "int2recv"),
        f(2405, "int2send"));
    base(
        23,
        "int4",
        4,
        true,
        'N',
        false,
        'i',
        'p',
        1007,
        0,
        f(42, "int4in"),
        f(43, "int4out"),
        f(2406, "int4recv"),
        f(2407, "int4send"));
    base(
        24,
        "regproc",
        4,
        true,
        'N',
        false,
        'i',
        'p',
        1008,
        0,
        f(44, "regprocin"),
        f(45, "regprocout"),
        f(2444, "regprocrecv"),
        f(2445, "regprocsend"));
    base(
        25,
        "text",
        -1,
        false,
        'S',
        true,
        'i',
        'x',
        1009,
        DEFAULT_COLLATION,
        f(46, "textin"),
        f(47, "textout"),
        f(2414, "textrecv"),
        f(2415, "textsend"));
    base(
        26,
        "oid",
        4,
        true,
        'N',
        true,
        'i',
        'p',
        1028,
        0,
        f(1798, "oidin"),
        f(1799, "oidout"),
        f(2418, "oidrecv"),
        f(2419, "oidsend"));
    base(
        700,
        "float4",
        4,
        true,
        'N',
        false,
        'i',
        'p',
        1021,
        0,
        f(200, "float4in"),
        f(201, "float4out"),
        f(2424, "float4recv"),
        f(2425, "float4send"));
    base(
        701,
        "float8",
        8,
        true,
        'N',
        true,
        'd',
        'p',
        1022,
        0,
        f(214, "float8in"),
        f(215, "float8out"),
        f(2426, "float8recv"),
        f(2427, "float8send"));
    base(
        1043,
        "varchar",
        -1,
        false,
        'S',
        false,
        'i',
        'x',
        1015,
        DEFAULT_COLLATION,
        f(1046, "varcharin"),
        f(1047, "varcharout"),
        f(2432, "varcharrecv"),
        f(2433, "varcharsend"),
        f(2915, "varchartypmodin"),
        f(2916, "varchartypmodout"));
    base(
        1082,
        "date",
        4,
        true,
        'D',
        false,
        'i',
        'p',
        1182,
        0,
        f(1084, "date_in"),
        f(1085, "date_out"),
        f(2468, "date_recv"),
        f(2469, "date_send"));
    base(
        1114,
        "timestamp",
        8,
        true,
        'D',
        false,
        'd',
        'p',
        1115,
        0,
        f(1312, "timestamp_in"),
        f(1313, "timestamp_out"),
        f(2474, "timestamp_recv"),
        f(2475, "timestamp_send"),
        f(2905, "timestamptypmodin"),
        f(2906, "timestamptypmodout"));
    base(
        1184,
        "timestamptz",
        8,
        true,
        'D',
        true,
        'd',
        'p',
        1185,
        0,
        f(1150, "timestamptz_in"),
        f(1151, "timestamptz_out"),
        f(2476, "timestamptz_recv"),
        f(2477, "timestamptz_send"),
        f(2907, "timestamptztypmodin"),
        f(2908, "timestamptztypmodout"));
    base(
        1700,
        "numeric",
        -1,
        false,
        'N',
        false,
        'i',
        'm',
        1231,
        0,
        f(1701, "numeric_in"),
        f(1702, "numeric_out"),
        f(2460, "numeric_recv"),
        f(2461, "numeric_send"),
        f(2917, "numerictypmodin"),
        f(2918, "numerictypmodout"));
    base(
        2206,
        "regtype",
        4,
        true,
        'N',
        false,
        'i',
        'p',
        2211,
        0,
        f(2220, "regtypein"),
        f(2221, "regtypeout"),
        f(2454, "regtyperecv"),
        f(2455, "regtypesend"));
    pseudo(
        705,
        "unknown",
        -2,
        false,
        'X',
        'c',
        'p',
        0,
        f(109, "unknownin"),
        f(110, "unknownout"),
        f(2416, "unknownrecv"),
        f(2417, "unknownsend"));
    pseudo(
        2249,
        "record",
        -1,
        false,
        'P',
        'd',
        'x',
        2287,
        f(2290, "record_in"),
        f(2291, "record_out"),
        f(2402, "record_recv"),
        f(2403, "record_send"));
    pseudo(
        2278,
        "void",
        4,
        true,
        'P',
        'i',
        'p',
        0,
        f(2298, "void_in"),
        f(2299, "void_out"),
        f(3120, "void_recv"),
        f(3121, "void_send"));
    PROCS.put(ARRAY_IN, "array_in");
    PROCS.put(ARRAY_OUT, "array_out");
    PROCS.put(ARRAY_RECV, "array_recv");
    PROCS.put(ARRAY_SEND, "array_send");
    PROCS.put(ARRAY_SUBSCRIPT, "array_subscript_handler");
    PROCS.put(RAW_ARRAY_SUBSCRIPT, "raw_array_subscript_handler");
    PROC_RESULT.put(ARRAY_OUT, CSTRING);
    PROC_RESULT.put(ARRAY_SEND, BYTEA);
    // the array types, after their element types
    for (TypeDef element : new ArrayList<>(TYPES.values())) {
      if (element.array != 0) {
        TYPES.put(
            element.array,
            new TypeDef(
                element.array,
                "_" + element.name,
                -1,
                false,
                'b',
                'A',
                false,
                element.align == 'd' ? 'd' : 'i',
                'x',
                element.oid,
                0,
                ARRAY_SUBSCRIPT,
                new long[] {ARRAY_IN, ARRAY_OUT, ARRAY_RECV, ARRAY_SEND, 0, 0},
                element.collation));
      }
    }

    // catalog relations with the OIDs PostgreSQL 14 gives them; the others get fixed OIDs
    // below the first user OID, in the (append-only) order of PgCatalogTables
    Map<String, Long> known = new HashMap<>();
    known.put("pg_type", 1247L);
    known.put("pg_attribute", 1249L);
    known.put("pg_proc", 1255L);
    known.put("pg_class", PG_CLASS);
    known.put("pg_database", 1262L);
    known.put("pg_am", 2601L);
    known.put("pg_attrdef", 2604L);
    known.put("pg_constraint", 2606L);
    known.put("pg_description", 2609L);
    known.put("pg_index", 2610L);
    known.put("pg_inherits", 2611L);
    known.put("pg_namespace", 2615L);
    known.put("pg_enum", 3501L);
    known.put("pg_collation", 3456L);
    known.put("pg_range", 3541L);
    long next = 12100;
    for (String table : PgCatalogTables.tables().keySet()) {
      Long oid = known.get(table);
      if (oid == null) {
        oid = next++;
      }
      boolean view =
          table.startsWith(PgCatalogTables.INFORMATION_SCHEMA_PREFIX)
              || table.equals("pg_roles")
              || table.equals("pg_user")
              || table.equals("pg_settings");
      RELATIONS.add(
          new SystemRelation(
              oid,
              table,
              table.startsWith(PgCatalogTables.INFORMATION_SCHEMA_PREFIX)
                  ? INFORMATION_SCHEMA
                  : PG_CATALOG,
              PgCatalogTables.postgresName(table),
              view ? 'v' : 'r'));
    }
  }

  private StaticCatalog() {}

  /** The type with this OID, if it is in the catalog. */
  public static Optional<TypeDef> type(long oid) {
    return Optional.ofNullable(TYPES.get(oid));
  }

  /** SQL names of types as {@code format_type} prints them, where they differ from typname. */
  private static final Map<String, String> SQL_NAMES = new HashMap<>();

  /** Names a type literal may use for a type ({@code 'integer'::regtype}). */
  private static final Map<String, String> ALIASES = new HashMap<>();

  static {
    SQL_NAMES.put("bool", "boolean");
    SQL_NAMES.put("char", "\"char\"");
    SQL_NAMES.put("int8", "bigint");
    SQL_NAMES.put("int2", "smallint");
    SQL_NAMES.put("int4", "integer");
    SQL_NAMES.put("float4", "real");
    SQL_NAMES.put("float8", "double precision");
    SQL_NAMES.put("varchar", "character varying");
    SQL_NAMES.put("timestamp", "timestamp without time zone");
    SQL_NAMES.put("timestamptz", "timestamp with time zone");
    for (Map.Entry<String, String> name : SQL_NAMES.entrySet()) {
      ALIASES.put(name.getValue(), name.getKey());
    }
    ALIASES.put("\"char\"", "char");
    ALIASES.put("int", "int4");
    ALIASES.put("float", "float8");
    ALIASES.put("character varying", "varchar");
    ALIASES.put("timestamp with time zone", "timestamptz");
  }

  /**
   * {@code format_type(oid, -1)} for every type of the catalog: the SQL name, arrays as the element
   * name with {@code []}.
   */
  public static Map<Long, String> formatTypeNames() {
    Map<Long, String> names = new LinkedHashMap<>();
    for (TypeDef type : TYPES.values()) {
      if (type.category == 'A') {
        TypeDef element = TYPES.get(type.elem);
        names.put(type.oid, SQL_NAMES.getOrDefault(element.name, element.name) + "[]");
      } else {
        names.put(type.oid, SQL_NAMES.getOrDefault(type.name, type.name));
      }
    }
    return names;
  }

  /**
   * The OID of a type given by name ({@code int4}, {@code integer}, {@code _int4}, {@code int4[]}).
   */
  public static Optional<Long> typeOid(String name) {
    String n = name.trim().replaceAll("\\s+", " ");
    if (n.startsWith("pg_catalog.")) {
      n = n.substring("pg_catalog.".length());
    }
    boolean array = n.endsWith("[]");
    if (array) {
      n = n.substring(0, n.length() - 2).trim();
    }
    n = ALIASES.getOrDefault(n, n);
    for (TypeDef type : TYPES.values()) {
      if (type.name.equals(n)) {
        if (!array) {
          return Optional.of(type.oid);
        }
        return type.array == 0 ? Optional.empty() : Optional.of(type.array);
      }
    }
    return Optional.empty();
  }

  /** The OID of a function of the catalog given by name ({@code array_in}). */
  public static Optional<Long> procOid(String name) {
    String n = name.startsWith("pg_catalog.") ? name.substring("pg_catalog.".length()) : name;
    for (Map.Entry<Long, String> proc : PROCS.entrySet()) {
      if (proc.getValue().equals(n)) {
        return Optional.of(proc.getKey());
      }
    }
    return Optional.empty();
  }

  /**
   * The OID of a catalog table or view, by schema ({@code null} = search {@code pg_catalog}) and
   * PostgreSQL name.
   */
  public static Optional<Long> systemRelationOid(String schema, String name) {
    for (SystemRelation relation : RELATIONS) {
      boolean schemaMatches =
          schema == null
              ? relation.namespace == PG_CATALOG
              : (schema.equals("pg_catalog") && relation.namespace == PG_CATALOG)
                  || (schema.equals("information_schema")
                      && relation.namespace == INFORMATION_SCHEMA);
      if (schemaMatches && relation.name.equals(name)) {
        return Optional.of(relation.oid);
      }
    }
    return Optional.empty();
  }

  static List<SystemRelation> systemRelations() {
    return Collections.unmodifiableList(RELATIONS);
  }

  /** Rows of {@code pg_type}. */
  static List<Map<String, Object>> types() {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (TypeDef t : TYPES.values()) {
      Map<String, Object> row = new HashMap<>();
      row.put("oid", t.oid);
      row.put("typname", t.name);
      row.put("typnamespace", PG_CATALOG);
      row.put("typowner", OWNER);
      row.put("typlen", t.len);
      row.put("typbyval", t.byval);
      row.put("typtype", String.valueOf(t.typtype));
      row.put("typcategory", String.valueOf(t.category));
      row.put("typispreferred", t.preferred);
      row.put("typisdefined", true);
      row.put("typdelim", ",");
      row.put("typrelid", 0L);
      row.put("typsubscript", t.subscript);
      row.put("typelem", t.elem);
      row.put("typarray", t.array);
      row.put("typinput", t.io[0]);
      row.put("typoutput", t.io[1]);
      row.put("typreceive", t.io[2]);
      row.put("typsend", t.io[3]);
      row.put("typmodin", t.io[4]);
      row.put("typmodout", t.io[5]);
      row.put("typanalyze", 0L);
      row.put("typalign", String.valueOf(t.align));
      row.put("typstorage", String.valueOf(t.storage));
      row.put("typnotnull", false);
      row.put("typbasetype", 0L);
      row.put("typtypmod", -1);
      row.put("typndims", 0);
      row.put("typcollation", t.collation);
      rows.add(row);
    }
    return rows;
  }

  /** Rows of {@code pg_proc}: the functions {@code pg_type} refers to. */
  static List<Map<String, Object>> procs() {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Map.Entry<Long, String> proc : PROCS.entrySet()) {
      Map<String, Object> row = new HashMap<>();
      row.put("oid", proc.getKey());
      row.put("proname", proc.getValue());
      row.put("pronamespace", PG_CATALOG);
      row.put("proowner", OWNER);
      row.put("prolang", 12L);
      row.put("procost", 1.0f);
      row.put("prorows", 0.0f);
      row.put("provariadic", 0L);
      row.put("prosupport", 0L);
      row.put("prokind", "f");
      row.put("prosecdef", false);
      row.put("proleakproof", false);
      row.put("proisstrict", true);
      row.put("proretset", false);
      row.put("provolatile", "i");
      row.put("proparallel", "s");
      row.put("pronargs", 1);
      row.put("pronargdefaults", 0);
      row.put("prorettype", PROC_RESULT.getOrDefault(proc.getKey(), 0L));
      row.put("prosrc", proc.getValue());
      rows.add(row);
    }
    return rows;
  }

  /** Rows of {@code pg_am}. */
  static List<Map<String, Object>> accessMethods() {
    List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(row("oid", 2L, "amname", "heap", "amhandler", 3L, "amtype", "t"));
    rows.add(row("oid", 403L, "amname", "btree", "amhandler", 330L, "amtype", "i"));
    return rows;
  }

  /** Rows of {@code pg_collation}: the built-in collations PostgreSQL always has. */
  static List<Map<String, Object>> collations() {
    List<Map<String, Object>> rows = new ArrayList<>();
    collation(rows, DEFAULT_COLLATION, "default", "d", -1, null);
    collation(rows, 950, "C", "c", -1, "C");
    collation(rows, 951, "POSIX", "c", -1, "POSIX");
    // UTF8 only; information_schema.character_sets names it as the default collation
    collation(rows, 12545, "ucs_basic", "c", 6, "C");
    return rows;
  }

  private static void collation(
      List<Map<String, Object>> rows,
      long oid,
      String name,
      String provider,
      int encoding,
      String locale) {
    Map<String, Object> row =
        row(
            "oid",
            oid,
            "collname",
            name,
            "collnamespace",
            PG_CATALOG,
            "collowner",
            OWNER,
            "collprovider",
            provider,
            "collisdeterministic",
            true,
            "collencoding",
            encoding);
    row.put("collcollate", locale);
    row.put("collctype", locale);
    rows.add(row);
  }

  /** Rows of {@code pg_settings}: the fixed parameters clients read. */
  static List<Map<String, Object>> settings() {
    List<Map<String, Object>> rows = new ArrayList<>();
    setting(rows, "block_size", "8192", "integer");
    setting(rows, "client_encoding", "UTF8", "string");
    setting(rows, "DateStyle", "ISO, MDY", "string");
    setting(rows, "default_transaction_read_only", "on", "bool");
    setting(rows, "integer_datetimes", "on", "bool");
    setting(rows, "IntervalStyle", "postgres", "enum");
    setting(rows, "max_identifier_length", "63", "integer");
    setting(rows, "max_index_keys", "32", "integer");
    setting(rows, "server_encoding", "UTF8", "string");
    setting(rows, "server_version", "14.0", "string");
    setting(rows, "server_version_num", "140000", "integer");
    setting(rows, "standard_conforming_strings", "on", "bool");
    setting(rows, "transaction_isolation", "read committed", "string");
    setting(rows, "transaction_read_only", "on", "bool");
    return rows;
  }

  private static void setting(
      List<Map<String, Object>> rows, String name, String value, String vartype) {
    Map<String, Object> row =
        row(
            "name",
            name,
            "setting",
            value,
            "category",
            "Preset Options",
            "context",
            "internal",
            "vartype",
            vartype,
            "source",
            "default");
    row.put("boot_val", value);
    row.put("reset_val", value);
    row.put("pending_restart", false);
    rows.add(row);
  }

  static Map<String, Object> row(Object... keysAndValues) {
    Map<String, Object> row = new HashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      row.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return row;
  }

  // ---------------------------------------------------------------------------- definitions

  private static final class Fn {
    final long oid;
    final String name;

    Fn(long oid, String name) {
      this.oid = oid;
      this.name = name;
    }
  }

  private static Fn f(long oid, String name) {
    return new Fn(oid, name);
  }

  private static void base(
      long oid,
      String name,
      int len,
      boolean byval,
      char category,
      boolean preferred,
      char align,
      char storage,
      long array,
      long collation,
      Fn... io) {
    // name is a fixed-length array of char (subscriptable), as in PostgreSQL
    long elem = name.equals("name") ? 18 : 0;
    long subscript = name.equals("name") ? RAW_ARRAY_SUBSCRIPT : 0;
    add(
        new TypeDef(
            oid,
            name,
            len,
            byval,
            'b',
            category,
            preferred,
            align,
            storage,
            elem,
            array,
            subscript,
            register(oid, io),
            collation));
  }

  private static void pseudo(
      long oid,
      String name,
      int len,
      boolean byval,
      char category,
      char align,
      char storage,
      long array,
      Fn... io) {
    add(
        new TypeDef(
            oid,
            name,
            len,
            byval,
            'p',
            category,
            false,
            align,
            storage,
            0,
            array,
            0,
            register(oid, io),
            0));
  }

  private static void add(TypeDef type) {
    TYPES.put(type.oid, type);
  }

  /** Registers the functions and returns in, out, recv, send, typmodin, typmodout. */
  private static long[] register(long type, Fn... io) {
    long[] oids = new long[6];
    for (int i = 0; i < io.length; i++) {
      oids[i] = io[i].oid;
      PROCS.put(io[i].oid, io[i].name);
    }
    // in and recv return the type, out returns cstring, send returns bytea
    PROC_RESULT.put(oids[0], type);
    PROC_RESULT.put(oids[1], CSTRING);
    PROC_RESULT.put(oids[2], type);
    PROC_RESULT.put(oids[3], BYTEA);
    return oids;
  }
}
