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

import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.postgresql.types.PgType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.zip.CRC32;

/**
 * The databases (PostgreSQL schemas) and tables one user may see, with stable OIDs. OIDs are
 * computed over all databases and tables, before the user's filter, so every user sees the same OID
 * for the same table: a CRC-32 of the name mapped to [16384, 2^31), on a collision the next free
 * value in name order.
 */
public final class CatalogSnapshot {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogSnapshot.class);

  /** The first OID PostgreSQL gives to user objects. */
  static final long FIRST_USER_OID = 16384;

  private static final long OID_RANGE = Integer.MAX_VALUE - FIRST_USER_OID;

  private static final Set<String> RESERVED = new HashSet<>();

  static {
    RESERVED.add("pg_catalog");
    RESERVED.add("information_schema");
  }

  private static final AtomicBoolean WARNED = new AtomicBoolean();

  /** A schema. */
  public static final class Namespace {
    final long oid;
    final String name;

    Namespace(long oid, String name) {
      this.oid = oid;
      this.name = name;
    }

    public long oid() {
      return oid;
    }

    public String name() {
      return name;
    }
  }

  /** A column. */
  public static final class Attribute {
    final String name;
    final int number;
    final PgType type;
    final boolean notNull;
    final String description;

    Attribute(String name, int number, PgType type, boolean notNull, String description) {
      this.name = name;
      this.number = number;
      this.type = type;
      this.notNull = notNull;
      this.description = description;
    }

    public String name() {
      return name;
    }

    public int number() {
      return number;
    }

    public PgType type() {
      return type;
    }

    public boolean notNull() {
      return notNull;
    }

    public String description() {
      return description;
    }
  }

  /** A table or view. */
  public static final class Relation {
    final long oid;
    final Namespace namespace;
    final String name;
    final char kind;
    final String description;
    final List<Attribute> attributes;

    Relation(
        long oid,
        Namespace namespace,
        String name,
        char kind,
        String description,
        List<Attribute> attributes) {
      this.oid = oid;
      this.namespace = namespace;
      this.name = name;
      this.kind = kind;
      this.description = description;
      this.attributes = attributes;
    }

    public long oid() {
      return oid;
    }

    public Namespace namespace() {
      return namespace;
    }

    public String name() {
      return name;
    }

    /** {@code r} for a table, {@code v} for a view. */
    public char kind() {
      return kind;
    }

    /** The table comment, or {@code null}. */
    public String description() {
      return description;
    }

    public List<Attribute> attributes() {
      return attributes;
    }
  }

  private final List<Namespace> namespaces;
  private final List<Relation> relations;

  private CatalogSnapshot(List<Namespace> namespaces, List<Relation> relations) {
    this.namespaces = namespaces;
    this.relations = relations;
  }

  /** The user's databases, in name order. */
  public List<Namespace> namespaces() {
    return namespaces;
  }

  /** The user's tables, in database and table order. */
  public List<Relation> relations() {
    return relations;
  }

  /**
   * Builds the snapshot.
   *
   * @param tables database → table name → table, as {@code DataNodeTableCache} returns them
   * @param canShowDatabase whether the user may see a database
   * @param canShowTable whether the user may see a table of a database
   */
  public static CatalogSnapshot of(
      Map<String, Map<String, TsTable>> tables,
      Predicate<String> canShowDatabase,
      BiPredicate<String, String> canShowTable) {
    Map<String, Map<String, TsTable>> sorted = new TreeMap<>();
    for (Map.Entry<String, Map<String, TsTable>> database : tables.entrySet()) {
      if (RESERVED.contains(database.getKey())) {
        if (WARNED.compareAndSet(false, true)) {
          LOGGER.warn(
              "The database {} is hidden from PostgreSQL clients: its name is a reserved"
                  + " PostgreSQL schema",
              database.getKey());
        }
        continue;
      }
      sorted.put(database.getKey(), new TreeMap<>(database.getValue()));
    }

    // OIDs over everything, then the filter
    Set<Long> used = new HashSet<>();
    Map<String, Long> databaseOids = new HashMap<>();
    Map<String, Long> tableOids = new HashMap<>();
    for (Map.Entry<String, Map<String, TsTable>> database : sorted.entrySet()) {
      databaseOids.put(database.getKey(), allocate("n:" + database.getKey(), used));
      for (String table : database.getValue().keySet()) {
        tableOids.put(
            database.getKey() + "." + table,
            allocate("r:" + database.getKey() + "." + table, used));
      }
    }

    List<Namespace> namespaces = new ArrayList<>();
    List<Relation> relations = new ArrayList<>();
    for (Map.Entry<String, Map<String, TsTable>> database : sorted.entrySet()) {
      String db = database.getKey();
      if (!canShowDatabase.test(db)) {
        continue;
      }
      Namespace namespace = new Namespace(databaseOids.get(db), db);
      namespaces.add(namespace);
      for (Map.Entry<String, TsTable> entry : database.getValue().entrySet()) {
        if (!canShowTable.test(db, entry.getKey())) {
          continue;
        }
        relations.add(relation(tableOids.get(db + "." + entry.getKey()), namespace, entry));
      }
    }
    return new CatalogSnapshot(
        Collections.unmodifiableList(namespaces), Collections.unmodifiableList(relations));
  }

  private static Relation relation(
      long oid, Namespace namespace, Map.Entry<String, TsTable> entry) {
    TsTable table = entry.getValue();
    List<Attribute> attributes = new ArrayList<>();
    for (TsTableColumnSchema column : table.getColumnList()) {
      Optional<PgType> type = PgType.of(column.getDataType());
      if (!type.isPresent()) {
        // OBJECT and other types without a PostgreSQL equivalent stay hidden
        continue;
      }
      TsTableColumnCategory category = column.getColumnCategory();
      String comment = column.getProps().get(TsTable.COMMENT_KEY);
      attributes.add(
          new Attribute(
              column.getColumnName(),
              attributes.size() + 1,
              type.get(),
              category == TsTableColumnCategory.TIME || category == TsTableColumnCategory.TAG,
              comment == null ? category.name() : category.name() + ": " + comment));
    }
    return new Relation(
        oid,
        namespace,
        entry.getKey(),
        TreeViewSchema.isTreeViewTable(table) ? 'v' : 'r',
        table.getPropValue(TsTable.COMMENT_KEY).orElse(null),
        Collections.unmodifiableList(attributes));
  }

  /** The OID for {@code key}: its hash, or the next free value after it. */
  static long allocate(String key, Set<Long> used) {
    CRC32 crc = new CRC32();
    crc.update(key.getBytes(StandardCharsets.UTF_8));
    long oid = FIRST_USER_OID + crc.getValue() % OID_RANGE;
    while (!used.add(oid)) {
      oid = oid + 1 >= FIRST_USER_OID + OID_RANGE ? FIRST_USER_OID : oid + 1;
    }
    return oid;
  }
}
