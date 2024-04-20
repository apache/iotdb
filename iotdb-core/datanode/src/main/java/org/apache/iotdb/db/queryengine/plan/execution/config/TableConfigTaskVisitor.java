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

package org.apache.iotdb.db.queryengine.plan.execution.config;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DescribeTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DropDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowTablesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.UseDBTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.relational.sql.tree.AstVisitor;
import org.apache.iotdb.db.relational.sql.tree.ColumnDefinition;
import org.apache.iotdb.db.relational.sql.tree.CreateDB;
import org.apache.iotdb.db.relational.sql.tree.CreateTable;
import org.apache.iotdb.db.relational.sql.tree.CurrentDatabase;
import org.apache.iotdb.db.relational.sql.tree.DataType;
import org.apache.iotdb.db.relational.sql.tree.DescribeTable;
import org.apache.iotdb.db.relational.sql.tree.DropDB;
import org.apache.iotdb.db.relational.sql.tree.DropTable;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.ShowDB;
import org.apache.iotdb.db.relational.sql.tree.ShowTables;
import org.apache.iotdb.db.relational.sql.tree.Use;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;

import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class TableConfigTaskVisitor extends AstVisitor<IConfigTask, MPPQueryContext> {

  private final IClientSession clientSession;

  private final Metadata metadata;

  public TableConfigTaskVisitor(IClientSession clientSession, Metadata metadata) {
    this.clientSession = clientSession;
    this.metadata = metadata;
  }

  @Override
  protected IConfigTask visitNode(Node node, MPPQueryContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + node.getClass().getName());
  }

  @Override
  protected IConfigTask visitCreateDB(CreateDB node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new CreateDBTask(node);
  }

  @Override
  protected IConfigTask visitUse(Use node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new UseDBTask(node, clientSession);
  }

  @Override
  protected IConfigTask visitDropDB(DropDB node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new DropDBTask(node);
  }

  @Override
  protected IConfigTask visitShowDB(ShowDB node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowDBTask(node);
  }

  @Override
  protected IConfigTask visitCreateTable(CreateTable node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    String database = clientSession.getDatabaseName();
    if (node.getName().getPrefix().isPresent()) {
      database = node.getName().getPrefix().get().toString();
    }
    if (database == null) {
      throw new SemanticException("unknown database");
    }
    TsTable table = new TsTable(node.getName().getSuffix());
    for (ColumnDefinition columnDefinition : node.getElements()) {
      ColumnDefinition.ColumnCategory category = columnDefinition.getColumnCategory();
      String columnName = columnDefinition.getName().getValue();
      if (table.getColumnSchema(columnName) != null) {
        throw new SemanticException(
            String.format("Columns in table shall not share the same name %s.", columnName));
      }
      TSDataType dataType = getDataType(columnDefinition.getType());
      switch (category) {
        case ID:
          table.addColumnSchema(new IdColumnSchema(columnName, dataType));
          break;
        case ATTRIBUTE:
          table.addColumnSchema(new AttributeColumnSchema(columnName, dataType));
          break;
        case TIME:
          break;
        case MEASUREMENT:
          table.addColumnSchema(
              new MeasurementColumnSchema(
                  columnName,
                  dataType,
                  getDefaultEncoding(dataType),
                  TSFileDescriptor.getInstance().getConfig().getCompressor()));
          break;
        default:
          throw new IllegalArgumentException();
      }
    }
    return new CreateTableTask(table, database, node.isIfNotExists());
  }

  private TSDataType getDataType(DataType dataType) {
    try {
      return getTSDataType(metadata.getType(toTypeSignature(dataType)));
    } catch (TypeNotFoundException e) {
      throw new SemanticException(String.format("Unknown type: %s", dataType));
    }
  }

  @Override
  protected IConfigTask visitDropTable(DropTable node, MPPQueryContext context) {
    return super.visitDropTable(node, context);
  }

  @Override
  protected IConfigTask visitShowTables(ShowTables node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    String database = clientSession.getDatabaseName();
    if (node.getDbName().isPresent()) {
      database = node.getDbName().get().toString();
    }
    if (database == null) {
      throw new SemanticException("unknown database");
    }
    return new ShowTablesTask(database);
  }

  @Override
  protected IConfigTask visitDescribeTable(DescribeTable node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    String database = clientSession.getDatabaseName();
    if (node.getTable().getPrefix().isPresent()) {
      database = node.getTable().getPrefix().get().toString();
    }
    if (database == null) {
      throw new SemanticException("unknown database");
    }
    return new DescribeTableTask(database, node.getTable().getSuffix());
  }

  @Override
  protected IConfigTask visitCurrentDatabase(CurrentDatabase node, MPPQueryContext context) {
    return super.visitCurrentDatabase(node, context);
  }
}
