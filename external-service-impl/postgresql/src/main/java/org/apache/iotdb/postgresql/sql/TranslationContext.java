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

package org.apache.iotdb.postgresql.sql;

import java.util.List;

/** What the translator needs to know about the session and the schema. */
public interface TranslationContext {

  /** The session's {@code search_path} schemas in order, folded, without {@code $user}. */
  List<String> searchPath();

  /** True if the database (PostgreSQL schema) has a table of this name (both folded). */
  boolean tableExists(String database, String table);

  /** The logged-in user. */
  String user();

  /** The value of {@code version()}. */
  String version();

  /** The value of {@code current_database()}. */
  String currentDatabase();

  /** The value of {@code pg_backend_pid()}. */
  int backendPid();

  /** The session time zone (date and time rules). */
  default java.time.ZoneId zone() {
    return java.time.ZoneOffset.UTC;
  }

  /** The database's timestamp precision (interval lengths, origins). */
  default org.apache.iotdb.postgresql.types.TimestampPrecision precision() {
    return org.apache.iotdb.postgresql.types.TimestampPrecision.of("ms");
  }
}
