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

package org.apache.iotdb.db.queryengine.plan.analyze.schema;

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.commons.utils.PathUtils.unQualifyDatabaseName;
import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.DATABASE_NOT_SPECIFIED;

public class SchemaValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValidator.class);

  public static void validate(
      ISchemaFetcher schemaFetcher, InsertBaseStatement insertStatement, MPPQueryContext context) {
    try {
      if (insertStatement instanceof InsertRowsStatement
          || insertStatement instanceof InsertMultiTabletsStatement
          || insertStatement instanceof InsertRowsOfOneDeviceStatement) {
        schemaFetcher.fetchAndComputeSchemaWithAutoCreate(
            insertStatement.getSchemaValidationList(), context);
      } else {
        schemaFetcher.fetchAndComputeSchemaWithAutoCreate(
            insertStatement.getSchemaValidation(), context);
      }
      insertStatement.updateAfterSchemaValidation(context);
    } catch (QueryProcessException e) {
      throw new SemanticException(e.getMessage());
    }
  }

  public static void validate(
      final Metadata metadata,
      final WrappedInsertStatement insertStatement,
      final MPPQueryContext context,
      AccessControl accessControl) {
    try {
      final InsertBaseStatement innerInsertStatement = insertStatement.getInnerTreeStatement();
      final boolean fromPipeBatch =
          innerInsertStatement instanceof InsertRowsStatement
              && ((InsertRowsStatement) innerInsertStatement).isFromPipeBatch();
      for (final QualifiedObjectName targetTable :
          resolveTargetTables(insertStatement, context, fromPipeBatch)) {
        accessControl.checkCanInsertIntoTable(
            context.getSession().getUserName(), targetTable, context);
      }
      insertStatement.validateTableSchema(metadata, context);
      insertStatement.updateAfterSchemaValidation(context);
      insertStatement.validateDeviceSchema(metadata, context);
      insertStatement.removeAttributeColumns();
    } catch (final QueryProcessException e) {
      throw new SemanticException(e.getMessage());
    }
  }

  /**
   * Resolves the target tables to check. Every row of a pipe batch is considered, since its rows
   * may target different tables; rows of any other insert share the same table, so the first row is
   * representative.
   */
  private static Set<QualifiedObjectName> resolveTargetTables(
      final WrappedInsertStatement insertStatement,
      final MPPQueryContext context,
      final boolean fromPipeBatch) {
    if (!(insertStatement instanceof InsertRows)) {
      return Collections.singleton(
          new QualifiedObjectName(
              unQualifyDatabaseName(insertStatement.getDatabase()),
              insertStatement.getTableName()));
    }

    final List<InsertRowStatement> rowStatements =
        ((InsertRows) insertStatement).getInnerTreeStatement().getInsertRowStatementList();
    if (!fromPipeBatch) {
      return rowStatements.isEmpty()
          ? Collections.emptySet()
          : Collections.singleton(resolveTargetTable(rowStatements.get(0), context));
    }

    final Set<QualifiedObjectName> targetTables = new LinkedHashSet<>();
    for (final InsertRowStatement rowStatement : rowStatements) {
      targetTables.add(resolveTargetTable(rowStatement, context));
    }
    return targetTables;
  }

  private static QualifiedObjectName resolveTargetTable(
      final InsertRowStatement rowStatement, final MPPQueryContext context) {
    final String database = AnalyzeUtils.getDatabaseName(rowStatement, context);
    if (database == null) {
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
    }
    return new QualifiedObjectName(unQualifyDatabaseName(database), rowStatement.getTableName());
  }

  public static ISchemaTree validate(
      ISchemaFetcher schemaFetcher,
      List<PartialPath> devicePaths,
      List<String[]> measurements,
      List<TSDataType[]> dataTypes,
      List<TSEncoding[]> encodings,
      List<CompressionType[]> compressionTypes,
      List<Boolean> isAlignedList,
      MPPQueryContext context) {
    return validate(
        schemaFetcher,
        devicePaths,
        measurements,
        dataTypes,
        encodings,
        compressionTypes,
        isAlignedList,
        true,
        context);
  }

  public static ISchemaTree validate(
      final ISchemaFetcher schemaFetcher,
      final List<PartialPath> devicePaths,
      final List<String[]> measurements,
      final List<TSDataType[]> dataTypes,
      final List<TSEncoding[]> encodings,
      final List<CompressionType[]> compressionTypes,
      final List<Boolean> isAlignedList,
      final boolean autoCreateSchema,
      final MPPQueryContext context) {
    return autoCreateSchema
        ? schemaFetcher.fetchSchemaListWithAutoCreate(
            devicePaths,
            measurements,
            dataTypes,
            encodings,
            compressionTypes,
            isAlignedList,
            context)
        : schemaFetcher.fetchSchemaList(devicePaths, measurements, context);
  }
}
