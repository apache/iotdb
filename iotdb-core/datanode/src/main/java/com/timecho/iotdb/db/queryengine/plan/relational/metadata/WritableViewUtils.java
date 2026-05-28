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

package com.timecho.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.WritableViewSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;

import com.timecho.iotdb.calc.plan.relational.metadata.TimechoCommonMetadataUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public final class WritableViewUtils {

  private WritableViewUtils() {
    // util class
  }

  public static String getSourceColumnName(
      final String viewColumnName, final Map<String, String> viewColumnToSourceColumnMap) {
    if (Objects.isNull(viewColumnName) || Objects.isNull(viewColumnToSourceColumnMap)) {
      return null;
    }
    final String sourceColumnName = viewColumnToSourceColumnMap.get(viewColumnName);
    return Objects.nonNull(sourceColumnName)
        ? sourceColumnName
        : viewColumnToSourceColumnMap.get(viewColumnName.toLowerCase(ENGLISH));
  }

  public static boolean requiresSourceColumnRewrite(
      final Map<String, String> viewColumnToSourceColumnMap) {
    if (Objects.isNull(viewColumnToSourceColumnMap) || viewColumnToSourceColumnMap.isEmpty()) {
      return false;
    }
    for (final Map.Entry<String, String> entry : viewColumnToSourceColumnMap.entrySet()) {
      if (Objects.isNull(entry.getKey())
          || Objects.isNull(entry.getValue())
          || !entry.getKey().equals(entry.getValue())) {
        return true;
      }
    }
    return false;
  }

  public static String getExistingSourceColumnName(
      final String writableViewDatabase,
      final WritableViewSchema writableViewSchema,
      final Set<String> sourceColumnNames,
      final String viewColumnName) {
    final String sourceColumnName =
        getSourceColumnName(viewColumnName, writableViewSchema.getViewColumnToSourceColumnMap());
    if (Objects.nonNull(sourceColumnName)) {
      if (!sourceColumnNames.contains(sourceColumnName)) {
        throwColumnNotExistsException(
            writableViewDatabase, writableViewSchema, viewColumnName, sourceColumnName);
      }
      return sourceColumnName;
    }

    final String normalizedViewColumnName = viewColumnName.toLowerCase(ENGLISH);
    if (sourceColumnNames.contains(viewColumnName)) {
      return viewColumnName;
    }
    if (sourceColumnNames.contains(normalizedViewColumnName)) {
      return normalizedViewColumnName;
    }

    throwColumnNotExistsException(writableViewDatabase, writableViewSchema, viewColumnName, null);
    return viewColumnName;
  }

  public static void throwColumnNotExistsException(
      final QualifiedObjectName writableViewName,
      final QualifiedObjectName sourceTableName,
      final String viewColumnName,
      final Map<String, String> viewColumnToSourceColumnMap) {
    throwColumnNotExistsException(
        writableViewName,
        sourceTableName,
        viewColumnName,
        getSourceColumnName(viewColumnName, viewColumnToSourceColumnMap));
  }

  public static void throwColumnNotExistsException(
      final QualifiedObjectName writableViewName,
      final QualifiedObjectName sourceTableName,
      final String viewColumnName,
      final String sourceColumnName) {
    TimechoCommonMetadataUtils.throwWritableViewColumnNotExistsException(
        writableViewName.getDatabaseName(),
        writableViewName.getObjectName(),
        sourceTableName.getDatabaseName(),
        sourceTableName.getObjectName(),
        viewColumnName,
        sourceColumnName);
  }

  public static void throwColumnNotExistsException(
      final String writableViewDatabase,
      final WritableViewSchema writableViewSchema,
      final String viewColumnName,
      final String sourceColumnName) {
    TimechoCommonMetadataUtils.throwWritableViewColumnNotExistsException(
        writableViewDatabase,
        writableViewSchema.getTableName(),
        writableViewSchema.getSourceTableName().getDatabaseName(),
        writableViewSchema.getSourceTableName().getObjectName(),
        viewColumnName,
        sourceColumnName);
  }

  public static Expression rewriteExpressionToSource(
      final Expression expression, final Map<String, String> viewColumnToSourceColumnMap) {
    if (Objects.isNull(expression)
        || Objects.isNull(viewColumnToSourceColumnMap)
        || !requiresSourceColumnRewrite(viewColumnToSourceColumnMap)) {
      return expression;
    }
    return ExpressionTreeRewriter.rewriteWith(
        new ExpressionRewriter<Void>() {
          @Override
          public Expression rewriteIdentifier(
              final Identifier node,
              final Void context,
              final ExpressionTreeRewriter<Void> treeRewriter) {
            final String sourceColumnName =
                getSourceColumnName(node.getValue(), viewColumnToSourceColumnMap);
            return Objects.nonNull(sourceColumnName) ? new Identifier(sourceColumnName) : node;
          }

          @Override
          public Expression rewriteSymbolReference(
              final SymbolReference node,
              final Void context,
              final ExpressionTreeRewriter<Void> treeRewriter) {
            final String sourceColumnName =
                getSourceColumnName(node.getName(), viewColumnToSourceColumnMap);
            return Objects.nonNull(sourceColumnName) ? new SymbolReference(sourceColumnName) : node;
          }
        },
        expression);
  }
}
