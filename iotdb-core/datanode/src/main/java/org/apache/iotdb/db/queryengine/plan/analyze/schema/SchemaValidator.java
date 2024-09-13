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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
      Metadata metadata, WrappedInsertStatement insertStatement, MPPQueryContext context) {
    try {
      insertStatement.toLowerCase();
      insertStatement.validateTableSchema(metadata, context);
      insertStatement.updateAfterSchemaValidation(context);
      insertStatement.validateDeviceSchema(metadata, context);
    } catch (QueryProcessException e) {
      throw new SemanticException(e.getMessage());
    }
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
    return schemaFetcher.fetchSchemaListWithAutoCreate(
        devicePaths, measurements, dataTypes, encodings, compressionTypes, isAlignedList, context);
  }
}
