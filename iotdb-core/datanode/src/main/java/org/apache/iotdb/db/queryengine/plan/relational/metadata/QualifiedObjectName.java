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

import com.google.errorprone.annotations.Immutable;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.MetadataUtil.checkObjectName;

@Immutable
public class QualifiedObjectName {
  private static final Pattern UNQUOTED_COMPONENT = Pattern.compile("[a-zA-Z0-9_]+");
  private static final String COMPONENT = UNQUOTED_COMPONENT.pattern() + "|\"([^\"]|\"\")*\"";
  private static final Pattern PATTERN =
      Pattern.compile("(?<database>" + COMPONENT + ")\\.(?<table>" + COMPONENT + ")");

  public static QualifiedObjectName valueOf(String name) {
    requireNonNull(name, "name is null");
    Matcher matcher = PATTERN.matcher(name);
    checkArgument(matcher.matches(), "Invalid name %s", name);
    return new QualifiedObjectName(
        unquoteIfNeeded(matcher.group("database")), unquoteIfNeeded(matcher.group("table")));
  }

  private final String dbName;

  // objectName represents tableName
  private final String objectName;

  public QualifiedObjectName(String dbName, String objectName) {
    checkObjectName(dbName, objectName);
    this.dbName = dbName;
    this.objectName = objectName;
  }

  public String getDatabaseName() {
    return dbName;
  }

  public String getObjectName() {
    return objectName;
  }

  public QualifiedTablePrefix asQualifiedTablePrefix() {
    return new QualifiedTablePrefix(dbName, objectName);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    QualifiedObjectName o = (QualifiedObjectName) obj;
    return Objects.equals(dbName, o.dbName) && Objects.equals(objectName, o.objectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbName, objectName);
  }

  @Override
  public String toString() {
    return quoteIfNeeded(dbName) + '.' + quoteIfNeeded(objectName);
  }

  private static String unquoteIfNeeded(String name) {
    if (name.isEmpty() || name.charAt(0) != '"') {
      return name;
    }
    checkArgument(name.charAt(name.length() - 1) == '"', "Invalid name: [%s]", name);
    return name.substring(1, name.length() - 1).replace("\"\"", "\"");
  }

  private static String quoteIfNeeded(String name) {
    if (UNQUOTED_COMPONENT.matcher(name).matches()) {
      return name;
    }
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }
}
