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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.*;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.*;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;

import org.apache.tsfile.enums.TSDataType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.COLUMN_TTL;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;

public class TableConfigTaskVisitor extends AstVisitor<IConfigTask, MPPQueryContext> {

  private static final String DATABASE_NOT_SPECIFIED = "database is not specified";

  private static final Set<String> TABLE_ALLOWED_PROPERTIES = new HashSet<>();

  static {
    TABLE_ALLOWED_PROPERTIES.add(COLUMN_TTL.toLowerCase(Locale.ENGLISH));
  }

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
  protected IConfigTask visitShowCluster(ShowCluster node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowClusterTask(node);
  }

  @Override
  protected IConfigTask visitShowRegions(ShowRegions node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowRegionsTask(node);
  }

  @Override
  protected IConfigTask visitShowDataNodes(ShowDataNodes node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowDataNodesTask(node);
  }

  protected IConfigTask visitShowConfigNodes(ShowConfigNodes node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowConfigNodesTask(node);
  }

  @Override
  protected IConfigTask visitCreateTable(CreateTable node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    String database = clientSession.getDatabaseName();
    if (node.getName().getPrefix().isPresent()) {
      database = node.getName().getPrefix().get().toString();
    }
    if (database == null) {
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
    }
    TsTable table = new TsTable(node.getName().getSuffix());
    Map<String, String> map = new HashMap<>();
    for (Property property : node.getProperties()) {
      String key = property.getName().getValue().toLowerCase(Locale.ENGLISH);
      if (TABLE_ALLOWED_PROPERTIES.contains(key) && !property.isSetToDefault()) {
        Expression value = property.getNonDefaultValue();
        if (!(value instanceof LongLiteral)) {
          throw new SemanticException(
              "TTL' value must be a LongLiteral, but now is: " + value.toString());
        }
        map.put(key, String.valueOf(((LongLiteral) value).getParsedValue()));
      }
    }
    table.setProps(map);

    for (ColumnDefinition columnDefinition : node.getElements()) {
      TsTableColumnCategory category = columnDefinition.getColumnCategory();
      String columnName = columnDefinition.getName().getValue();
      if (table.getColumnSchema(columnName) != null) {
        throw new SemanticException(
            String.format("Columns in table shall not share the same name %s.", columnName));
      }
      TSDataType dataType = getDataType(columnDefinition.getType());
      TableHeaderSchemaValidator.generateColumnSchema(table, category, columnName, dataType);
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
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
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
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
    }
    return new DescribeTableTask(database, node.getTable().getSuffix());
  }

  @Override
  protected IConfigTask visitCurrentDatabase(CurrentDatabase node, MPPQueryContext context) {
    return super.visitCurrentDatabase(node, context);
  }
}
