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

import java.util.Locale;

/** PostgreSQL identifier rules: unquoted names fold to lower case, quoted names keep theirs. */
final class Identifiers {

  private Identifiers() {}

  /** The name an identifier as written in SQL stands for. */
  static String fold(String identifier) {
    if (identifier == null) {
      return null;
    }
    if (identifier.length() >= 2 && identifier.startsWith("\"") && identifier.endsWith("\"")) {
      return identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"");
    }
    return identifier.toLowerCase(Locale.ROOT);
  }

  /** The name as a double-quoted table-model identifier. */
  static String quote(String name) {
    return '"' + name.replace("\"", "\"\"") + '"';
  }

  /** A string literal of the table model. */
  static String literal(String value) {
    return '\'' + value.replace("'", "''") + '\'';
  }
}
