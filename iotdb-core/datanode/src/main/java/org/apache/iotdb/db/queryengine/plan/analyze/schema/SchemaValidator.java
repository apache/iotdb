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
      for (final QualifiedObjectName targetTable : getTargetTables(insertStatement, context)) {
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

  private static Set<QualifiedObjectName> getTargetTables(
      final WrappedInsertStatement insertStatement, final MPPQueryContext context) {
    final Set<QualifiedObjectName> targetTables = new LinkedHashSet<>();
    if (insertStatement instanceof InsertRows) {
      for (final InsertRowStatement rowStatement :
          ((InsertRows) insertStatement).getInnerTreeStatement().getInsertRowStatementList()) {
        final String database = AnalyzeUtils.getDatabaseName(rowStatement, context);
        if (database == null) {
          throw new SemanticException(DATABASE_NOT_SPECIFIED);
        }
        targetTables.add(
            new QualifiedObjectName(unQualifyDatabaseName(database), rowStatement.getTableName()));
      }
    } else {
      targetTables.add(
          new QualifiedObjectName(
              unQualifyDatabaseName(insertStatement.getDatabase()),
              insertStatement.getTableName()));
    }
    return targetTables;
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
