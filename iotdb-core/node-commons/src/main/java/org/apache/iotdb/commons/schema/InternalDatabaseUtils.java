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

package org.apache.iotdb.commons.schema;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.Audit;

public final class InternalDatabaseUtils {

  private InternalDatabaseUtils() {}

  /** Whether the given database (or one of its descendants) is an internal database. */
  public static boolean isInternalDatabase(final String databaseName) {
    return isSystemDatabase(databaseName) || Audit.isAuditDatabase(databaseName);
  }

  /** Whether the given database (or one of its descendants) is {@code root.__system}. */
  public static boolean isSystemDatabase(final String databaseName) {
    return isDatabaseOrDescendant(databaseName, SchemaConstant.SYSTEM_DATABASE);
  }

  /** Whether the device or measurement path belongs to {@code root.__system}. */
  public static boolean isSystemDatabasePath(final PartialPath path) {
    return path != null && isSystemDatabase(path.getFullPath());
  }

  // Internal database names are matched case-insensitively: root.__System is also internal.
  private static boolean isDatabaseOrDescendant(
      final String databaseName, final String internalDatabase) {
    return databaseName != null
        && databaseName.regionMatches(true, 0, internalDatabase, 0, internalDatabase.length())
        && (databaseName.length() == internalDatabase.length()
            || databaseName.charAt(internalDatabase.length()) == '.');
  }
}
