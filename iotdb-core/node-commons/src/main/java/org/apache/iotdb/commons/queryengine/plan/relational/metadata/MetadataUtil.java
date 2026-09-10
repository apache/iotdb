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

package org.apache.iotdb.commons.queryengine.plan.relational.metadata;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.SchemaMessages;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;

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
    checkLowerCase(dbName, "dbName");
    checkLowerCase(objectName, "objectName");
  }

  public static String checkLowerCase(String value, String name) {
    if (value == null) {
      throw new NullPointerException(format(SchemaMessages.S_IS_NULL, name));
    }
    checkArgument(
        value.equals(value.toLowerCase(ENGLISH)),
        SchemaMessages.EXCEPTION_ARG_IS_NOT_LOWERCASE_COLON_ARG_D78298F6,
        name,
        value);
    return value;
  }

  public static QualifiedObjectName createQualifiedObjectName(
      final SessionInfo session, final QualifiedName name) {
    requireNonNull(session, SchemaMessages.EXCEPTION_SESSION_IS_NULL_6CF0F47D);
    requireNonNull(name, SchemaMessages.EXCEPTION_NAME_IS_NULL_C8B35959);
    if (name.getParts().size() > 2) {
      throw new SemanticException(String.format(SchemaMessages.TOO_MANY_DOTS_IN_TABLE_NAME, name));
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
                            SchemaMessages
                                .EXCEPTION_DATABASE_MUST_SPECIFIED_SESSION_DATABASE_NOT_SET_CBF6F21F));

    return new QualifiedObjectName(databaseName, objectName);
  }
}
