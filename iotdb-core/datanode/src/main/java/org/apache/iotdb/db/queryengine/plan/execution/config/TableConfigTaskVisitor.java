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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableAddColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableSetPropertiesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DescribeTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DropDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowConfigNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowDataNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowTablesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.UseDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.FlushTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.SetConfigurationTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Flush;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetConfiguration;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCluster;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowConfigNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDataNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowRegions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetConfigurationStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MAX_DATABASE_NAME_LENGTH;
import static org.apache.iotdb.commons.schema.table.TsTable.TABLE_ALLOWED_PROPERTIES_2_DEFAULT_VALUE_MAP;
import static org.apache.iotdb.commons.schema.table.TsTable.TTL_PROPERTY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.DATA_REGION_GROUP_NUM_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.DATA_REPLICATION_FACTOR_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.SCHEMA_REGION_GROUP_NUM_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.SCHEMA_REPLICATION_FACTOR_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.TIME_PARTITION_INTERVAL_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.TTL_KEY;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;
import static org.apache.iotdb.db.utils.constant.SqlConstant.ROOT;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR_CHAR;

public class TableConfigTaskVisitor extends AstVisitor<IConfigTask, MPPQueryContext> {

  private static final String DATABASE_NOT_SPECIFIED = "database is not specified";

  private final IClientSession clientSession;

  private final Metadata metadata;

  public TableConfigTaskVisitor(final IClientSession clientSession, final Metadata metadata) {
    this.clientSession = clientSession;
    this.metadata = metadata;
  }

  @Override
  protected IConfigTask visitNode(final Node node, final MPPQueryContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + node.getClass().getName());
  }

  @Override
  protected IConfigTask visitCreateDB(final CreateDB node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    final TDatabaseSchema schema = new TDatabaseSchema();

    final String dbName = node.getDbName();
    // Check database length here
    // We need to calculate the database name without "root."
    if (dbName.contains(PATH_SEPARATOR) || dbName.length() > MAX_DATABASE_NAME_LENGTH) {
      throw new SemanticException(
          new IllegalPathException(
              node.getDbName(),
              dbName.contains(PATH_SEPARATOR)
                  ? "The database name shall not contain '.'"
                  : "the length of database name shall not exceed " + MAX_DATABASE_NAME_LENGTH));
    }
    schema.setName(ROOT + PATH_SEPARATOR_CHAR + node.getDbName());

    for (final Property property : node.getProperties()) {
      final String key = property.getName().getValue().toLowerCase(Locale.ENGLISH);
      if (property.isSetToDefault()) {
        switch (key) {
          case TTL_KEY:
          case SCHEMA_REPLICATION_FACTOR_KEY:
          case DATA_REPLICATION_FACTOR_KEY:
          case TIME_PARTITION_INTERVAL_KEY:
          case SCHEMA_REGION_GROUP_NUM_KEY:
          case DATA_REGION_GROUP_NUM_KEY:
            break;
          default:
            throw new SemanticException("Unsupported database property key: " + key);
        }
        continue;
      }

      final Object value = property.getNonDefaultValue();

      switch (key) {
        case TTL_KEY:
          schema.setTTL(parseLongFromLiteral(value, TTL_KEY));
          break;
        case SCHEMA_REPLICATION_FACTOR_KEY:
          schema.setSchemaReplicationFactor(
              parseIntFromLiteral(value, SCHEMA_REPLICATION_FACTOR_KEY));
          break;
        case DATA_REPLICATION_FACTOR_KEY:
          schema.setDataReplicationFactor(parseIntFromLiteral(value, DATA_REPLICATION_FACTOR_KEY));
          break;
        case TIME_PARTITION_INTERVAL_KEY:
          schema.setTimePartitionInterval(parseLongFromLiteral(value, TIME_PARTITION_INTERVAL_KEY));
          break;
        case SCHEMA_REGION_GROUP_NUM_KEY:
          schema.setMinSchemaRegionGroupNum(
              parseIntFromLiteral(value, SCHEMA_REGION_GROUP_NUM_KEY));
          break;
        case DATA_REGION_GROUP_NUM_KEY:
          schema.setMinDataRegionGroupNum(parseIntFromLiteral(value, DATA_REGION_GROUP_NUM_KEY));
          break;
        default:
          throw new SemanticException("Unsupported database property key: " + key);
      }
    }
    return new CreateDBTask(schema, node.isSetIfNotExists());
  }

  @Override
  protected IConfigTask visitUse(final Use node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new UseDBTask(node, clientSession);
  }

  @Override
  protected IConfigTask visitDropDB(final DropDB node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new DropDBTask(node);
  }

  @Override
  protected IConfigTask visitShowDB(final ShowDB node, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowDBTask(node);
  }

  @Override
  protected IConfigTask visitShowCluster(
      final ShowCluster showCluster, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    ShowClusterStatement treeStatement = new ShowClusterStatement();
    treeStatement.setDetails(showCluster.getDetails().orElse(false));
    return new ShowClusterTask(treeStatement);
  }

  @Override
  protected IConfigTask visitShowRegions(
      final ShowRegions showRegions, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    ShowRegionStatement treeStatement = new ShowRegionStatement();
    treeStatement.setRegionType(showRegions.getRegionType());
    treeStatement.setStorageGroups(showRegions.getDatabases());
    treeStatement.setNodeIds(showRegions.getNodeIds());
    return new ShowRegionTask(treeStatement);
  }

  @Override
  protected IConfigTask visitShowDataNodes(
      final ShowDataNodes showDataNodesStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowDataNodesTask(showDataNodesStatement);
  }

  protected IConfigTask visitShowConfigNodes(
      final ShowConfigNodes showConfigNodesStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowConfigNodesTask(showConfigNodesStatement);
  }

  @Override
  protected IConfigTask visitCreateTable(final CreateTable node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getName());

    final TsTable table = new TsTable(databaseTablePair.getRight());

    table.setProps(convertPropertiesToMap(node.getProperties(), false));

    // TODO: Place the check at statement analyzer
    for (final ColumnDefinition columnDefinition : node.getElements()) {
      final TsTableColumnCategory category = columnDefinition.getColumnCategory();
      final String columnName = columnDefinition.getName().getValue();
      if (table.getColumnSchema(columnName) != null) {
        throw new SemanticException(
            String.format("Columns in table shall not share the same name %s.", columnName));
      }
      final TSDataType dataType = getDataType(columnDefinition.getType());
      table.addColumnSchema(
          TableHeaderSchemaValidator.generateColumnSchema(category, columnName, dataType));
    }
    return new CreateTableTask(table, databaseTablePair.getLeft(), node.isIfNotExists());
  }

  @Override
  protected IConfigTask visitAddColumn(final AddColumn node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTableName());

    final ColumnDefinition definition = node.getColumn();
    return new AlterTableAddColumnTask(
        databaseTablePair.getLeft(),
        databaseTablePair.getRight(),
        Collections.singletonList(
            TableHeaderSchemaValidator.generateColumnSchema(
                definition.getColumnCategory(),
                definition.getName().getValue(),
                getDataType(definition.getType()))),
        context.getQueryId().getId(),
        node.tableIfExists(),
        node.columnIfNotExists());
  }

  @Override
  protected IConfigTask visitSetProperties(
      final SetProperties node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getName());

    return new AlterTableSetPropertiesTask(
        databaseTablePair.getLeft(),
        databaseTablePair.getRight(),
        convertPropertiesToMap(node.getProperties(), true),
        context.getQueryId().getId(),
        node.ifExists());
  }

  public Pair<String, String> splitQualifiedName(final QualifiedName name) {
    String database = clientSession.getDatabaseName();
    if (name.getPrefix().isPresent()) {
      database = name.getPrefix().get().toString();
    }
    if (database == null) {
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
    }
    return new Pair<>(database, name.getSuffix());
  }

  private Map<String, String> convertPropertiesToMap(
      final List<Property> propertyList, final boolean serializeDefault) {
    final Map<String, String> map = new HashMap<>();
    for (final Property property : propertyList) {
      final String key = property.getName().getValue().toLowerCase(Locale.ENGLISH);
      if (TABLE_ALLOWED_PROPERTIES_2_DEFAULT_VALUE_MAP.containsKey(key)) {
        if (!property.isSetToDefault()) {
          final Expression value = property.getNonDefaultValue();
          if (value instanceof Literal
              && Objects.equals(
                  ((Literal) value).getTsValue(),
                  TABLE_ALLOWED_PROPERTIES_2_DEFAULT_VALUE_MAP.get(key))) {
            // Ignore default values
            continue;
          }
          // TODO: support validation for other properties
          map.put(
              key,
              String.valueOf(
                  parseLongFromLiteral(value, TTL_PROPERTY.toLowerCase(Locale.ENGLISH))));
        } else if (serializeDefault) {
          map.put(key, null);
        }
      } else {
        throw new SemanticException("Table property '" + key + "' is currently not allowed.");
      }
    }
    return map;
  }

  private TSDataType getDataType(final DataType dataType) {
    try {
      return getTSDataType(metadata.getType(toTypeSignature(dataType)));
    } catch (final TypeNotFoundException e) {
      throw new SemanticException(String.format("Unknown type: %s", dataType));
    }
  }

  @Override
  protected IConfigTask visitDropTable(DropTable node, MPPQueryContext context) {
    return super.visitDropTable(node, context);
  }

  @Override
  protected IConfigTask visitShowTables(final ShowTables node, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    String database = clientSession.getDatabaseName();
    if (node.getDbName().isPresent()) {
      database = node.getDbName().get().getValue();
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

  @Override
  protected IConfigTask visitFlush(Flush node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new FlushTask(((FlushStatement) node.getInnerTreeStatement()));
  }

  @Override
  protected IConfigTask visitSetConfiguration(SetConfiguration node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new SetConfigurationTask(((SetConfigurationStatement) node.getInnerTreeStatement()));
  }

  private long parseLongFromLiteral(final Object value, final String name) {
    if (!(value instanceof LongLiteral)) {
      throw new SemanticException(
          name
              + " value must be a LongLiteral, but now is "
              + (Objects.nonNull(value) ? value.getClass().getSimpleName() : null)
              + ", value: "
              + value);
    }
    final long parsedValue = ((LongLiteral) value).getParsedValue();
    if (parsedValue < 0) {
      throw new SemanticException(
          name + " value must be equal to or greater than 0, but now is: " + value);
    }
    return parsedValue;
  }

  private int parseIntFromLiteral(final Object value, final String name) {
    if (!(value instanceof LongLiteral)) {
      throw new SemanticException(
          name
              + " value must be a LongLiteral, but now is "
              + (Objects.nonNull(value) ? value.getClass().getSimpleName() : null)
              + ", value: "
              + value);
    }
    final long parsedValue = ((LongLiteral) value).getParsedValue();
    if (parsedValue < 0) {
      throw new SemanticException(
          name + " value must be equal to or greater than 0, but now is: " + value);
    } else if (parsedValue > Integer.MAX_VALUE) {
      throw new SemanticException(
          name + " value must be lower than " + Integer.MAX_VALUE + ", but now is: " + value);
    }
    return (int) parsedValue;
  }
}
