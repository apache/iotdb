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

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreateFunctionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreatePipePluginTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropFunctionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropPipePluginTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterIdTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowFunctionsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowPipePluginsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowVariablesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableAddColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableDropColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableRenameColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableRenameTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableSetPropertiesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ClearCacheTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DeleteDeviceTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DescribeTableDetailsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DescribeTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DropDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DropTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowAINodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowConfigNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowDataNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowTablesDetailsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowTablesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.UseDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentDatabaseTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentSqlDialectTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentTimestampTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentUserTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowVersionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.FlushTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.KillQueryTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.SetConfigurationTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.AlterPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.CreatePipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.DropPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.ShowPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.StartPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.StopPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.CreateTopicTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.DropTopicTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.ShowSubscriptionsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.ShowTopicsTask;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analyzer;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ClearCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DeleteDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Flush;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.KillQuery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetConfiguration;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowAINodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCluster;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowClusterId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowConfigNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentTimestamp;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDataNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipePlugins;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowRegions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTopics;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVariables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVersion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite.StatementRewrite;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetConfigurationStatement;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;

import java.security.AccessControlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MAX_DATABASE_NAME_LENGTH;
import static org.apache.iotdb.commons.conf.IoTDBConstant.TTL_INFINITE;
import static org.apache.iotdb.commons.schema.table.TsTable.TABLE_ALLOWED_PROPERTIES;
import static org.apache.iotdb.commons.schema.table.TsTable.TTL_PROPERTY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.DATA_REGION_GROUP_NUM_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.SCHEMA_REGION_GROUP_NUM_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.TIME_PARTITION_INTERVAL_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.TTL_KEY;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;
import static org.apache.iotdb.db.utils.constant.SqlConstant.ROOT;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR_CHAR;

public class TableConfigTaskVisitor extends AstVisitor<IConfigTask, MPPQueryContext> {

  public static final String DATABASE_NOT_SPECIFIED = "database is not specified";

  private final IClientSession clientSession;

  private final Metadata metadata;

  private final AccessControl accessControl;

  public TableConfigTaskVisitor(
      final IClientSession clientSession,
      final Metadata metadata,
      final AccessControl accessControl) {
    this.clientSession = clientSession;
    this.metadata = metadata;
    this.accessControl = accessControl;
  }

  @Override
  protected IConfigTask visitNode(final Node node, final MPPQueryContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + node.getClass().getName());
  }

  @Override
  protected IConfigTask visitCreateDB(final CreateDB node, final MPPQueryContext context) {
    return visitDatabaseStatement(node, context);
  }

  @Override
  protected IConfigTask visitAlterDB(final AlterDB node, final MPPQueryContext context) {
    return visitDatabaseStatement(node, context);
  }

  private IConfigTask visitDatabaseStatement(
      final DatabaseStatement node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    final TDatabaseSchema schema = new TDatabaseSchema();
    schema.setIsTableModel(true);

    final String dbName = node.getDbName();
    validateDatabaseName(dbName);

    accessControl.checkCanCreateDatabase(context.getSession().getUserName(), dbName);

    schema.setName(ROOT + PATH_SEPARATOR_CHAR + dbName);

    for (final Property property : node.getProperties()) {
      final String key = property.getName().getValue().toLowerCase(Locale.ENGLISH);
      if (property.isSetToDefault()) {
        switch (key) {
          case TIME_PARTITION_INTERVAL_KEY:
          case SCHEMA_REGION_GROUP_NUM_KEY:
          case DATA_REGION_GROUP_NUM_KEY:
            break;
          case TTL_KEY:
            if (node.getType() == DatabaseSchemaStatement.DatabaseSchemaStatementType.ALTER) {
              schema.setTTL(Long.MAX_VALUE);
            }
            break;
          default:
            throw new SemanticException("Unsupported database property key: " + key);
        }
        continue;
      }

      final Object value = property.getNonDefaultValue();

      switch (key) {
        case TTL_KEY:
          final Optional<String> strValue = parseStringFromLiteralIfBinary(value);
          if (strValue.isPresent()) {
            if (!strValue.get().equalsIgnoreCase(TTL_INFINITE)) {
              throw new SemanticException(
                  "ttl value must be 'INF' or a long literal, but now is: " + value);
            }
            if (node.getType() == DatabaseSchemaStatement.DatabaseSchemaStatementType.ALTER) {
              schema.setTTL(Long.MAX_VALUE);
            }
            break;
          }
          schema.setTTL(parseLongFromLiteral(value, TTL_KEY));
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
    return node.getType() == DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE
        ? new CreateDBTask(schema, node.exists())
        : new AlterDBTask(schema, node.exists());
  }

  @Override
  protected IConfigTask visitUse(final Use node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkCanShowOrUseDatabase(
        context.getSession().getUserName(), node.getDatabaseId().getValue());
    return new UseDBTask(node, clientSession);
  }

  @Override
  protected IConfigTask visitDropDB(final DropDB node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkCanDropDatabase(
        context.getSession().getUserName(), node.getDbName().getValue());
    return new DropDBTask(node);
  }

  @Override
  protected IConfigTask visitShowDB(final ShowDB node, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowDBTask(
        node,
        databaseName -> {
          try {
            accessControl.checkCanShowOrUseDatabase(
                context.getSession().getUserName(), databaseName);
            return true;
          } catch (AccessControlException e) {
            return false;
          }
        });
  }

  @Override
  protected IConfigTask visitShowCluster(
      final ShowCluster showCluster, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
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
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    final ShowRegionStatement treeStatement = new ShowRegionStatement();
    treeStatement.setRegionType(showRegions.getRegionType());
    treeStatement.setStorageGroups(showRegions.getDatabases());
    treeStatement.setNodeIds(showRegions.getNodeIds());
    return new ShowRegionTask(treeStatement, true);
  }

  @Override
  protected IConfigTask visitShowDataNodes(
      final ShowDataNodes showDataNodesStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
    return new ShowDataNodesTask();
  }

  @Override
  protected IConfigTask visitShowConfigNodes(
      final ShowConfigNodes showConfigNodesStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
    return new ShowConfigNodesTask();
  }

  @Override
  protected IConfigTask visitShowAINodes(
      final ShowAINodes showAINodesStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
    return new ShowAINodesTask();
  }

  @Override
  protected IConfigTask visitClearCache(
      final ClearCache clearCacheStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
    return new ClearCacheTask(clearCacheStatement);
  }

  @Override
  protected IConfigTask visitCreateTable(final CreateTable node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getName());

    final TsTable table = new TsTable(databaseTablePair.getRight());

    table.setProps(convertPropertiesToMap(node.getProperties(), false));

    // TODO: Place the check at statement analyzer
    boolean hasTimeColumn = false;
    for (final ColumnDefinition columnDefinition : node.getElements()) {
      final TsTableColumnCategory category = columnDefinition.getColumnCategory();
      final String columnName = columnDefinition.getName().getValue();
      final TSDataType dataType = getDataType(columnDefinition.getType());
      if (checkTimeColumnIdempotent(category, columnName, dataType) && !hasTimeColumn) {
        hasTimeColumn = true;
        continue;
      }
      if (table.getColumnSchema(columnName) != null) {
        throw new SemanticException(
            String.format("Columns in table shall not share the same name %s.", columnName));
      }
      table.addColumnSchema(
          TableHeaderSchemaValidator.generateColumnSchema(category, columnName, dataType));
    }
    return new CreateTableTask(table, databaseTablePair.getLeft(), node.isIfNotExists());
  }

  private boolean checkTimeColumnIdempotent(
      final TsTableColumnCategory category, final String columnName, final TSDataType dataType) {
    if (category == TsTableColumnCategory.TIME || columnName.equals(TsTable.TIME_COLUMN_NAME)) {
      if (category == TsTableColumnCategory.TIME
          && columnName.equals(TsTable.TIME_COLUMN_NAME)
          && dataType == TSDataType.TIMESTAMP) {
        return true;
      } else if (dataType == TSDataType.TIMESTAMP) {
        throw new SemanticException(
            "The time column category shall be bounded with column name 'time'.");
      } else {
        throw new SemanticException("The time column's type shall be 'timestamp'.");
      }
    }

    return false;
  }

  @Override
  protected IConfigTask visitRenameTable(final RenameTable node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getSource());

    final String oldName = databaseTablePair.getRight();
    final String newName = node.getTarget().getValue();
    if (oldName.equals(newName)) {
      throw new SemanticException("The table's old name shall not be equal to the new one.");
    }

    return new AlterTableRenameTableTask(
        databaseTablePair.getLeft(),
        databaseTablePair.getRight(),
        node.getTarget().getValue(),
        context.getQueryId().getId(),
        node.tableIfExists());
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
  protected IConfigTask visitRenameColumn(final RenameColumn node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTable());

    final String oldName = node.getSource().getValue();
    final String newName = node.getTarget().getValue();
    if (oldName.equals(newName)) {
      throw new SemanticException("The column's old name shall not be equal to the new one.");
    }

    return new AlterTableRenameColumnTask(
        databaseTablePair.getLeft(),
        databaseTablePair.getRight(),
        node.getSource().getValue(),
        node.getTarget().getValue(),
        context.getQueryId().getId(),
        node.tableIfExists(),
        node.columnIfExists());
  }

  @Override
  protected IConfigTask visitDropColumn(final DropColumn node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTable());

    return new AlterTableDropColumnTask(
        databaseTablePair.getLeft(),
        databaseTablePair.getRight(),
        node.getField().getValue(),
        context.getQueryId().getId(),
        node.tableIfExists(),
        node.columnIfExists());
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

  public static void validateDatabaseName(final String dbName) throws SemanticException {
    // Check database length here
    // We need to calculate the database name without "root."
    if (dbName.contains(PATH_SEPARATOR)
        || !IoTDBConfig.STORAGE_GROUP_PATTERN.matcher(dbName).matches()
        || dbName.length() > MAX_DATABASE_NAME_LENGTH) {
      throw new SemanticException(
          new IllegalPathException(
              dbName,
              dbName.length() > MAX_DATABASE_NAME_LENGTH
                  ? "the length of database name shall not exceed " + MAX_DATABASE_NAME_LENGTH
                  : "the database name can only contain english or chinese characters, numbers, backticks and underscores."));
    }
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
      if (TABLE_ALLOWED_PROPERTIES.contains(key)) {
        if (!property.isSetToDefault()) {
          final Expression value = property.getNonDefaultValue();
          final Optional<String> strValue = parseStringFromLiteralIfBinary(value);
          if (strValue.isPresent()) {
            if (!strValue.get().equalsIgnoreCase(TTL_INFINITE)) {
              throw new SemanticException(
                  "ttl value must be 'INF' or a long literal, but now is: " + value);
            }
            map.put(key, strValue.get().toUpperCase(Locale.ENGLISH));
            continue;
          }
          // TODO: support validation for other properties
          map.put(key, String.valueOf(parseLongFromLiteral(value, TTL_PROPERTY)));
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
  protected IConfigTask visitDropTable(final DropTable node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTableName());
    return new DropTableTask(
        databaseTablePair.getLeft(),
        databaseTablePair.getRight(),
        context.getQueryId().getId(),
        node.isExists());
  }

  @Override
  protected IConfigTask visitDeleteDevice(final DeleteDevice node, final MPPQueryContext context) {
    new Analyzer(
            context,
            context.getSession(),
            new StatementAnalyzerFactory(metadata, null, accessControl),
            Collections.emptyList(),
            Collections.emptyMap(),
            StatementRewrite.NOOP,
            WarningCollector.NOOP)
        .analyze(node);
    return new DeleteDeviceTask(node, context.getQueryId().getId(), context.getSession());
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
    return node.isDetails() ? new ShowTablesDetailsTask(database) : new ShowTablesTask(database);
  }

  @Override
  protected IConfigTask visitDescribeTable(
      final DescribeTable node, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    String database = clientSession.getDatabaseName();
    if (node.getTable().getPrefix().isPresent()) {
      database = node.getTable().getPrefix().get().toString();
    }
    if (database == null) {
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
    }
    final String tableName = node.getTable().getSuffix();
    return node.isDetails()
        ? new DescribeTableDetailsTask(database, tableName)
        : new DescribeTableTask(database, tableName);
  }

  @Override
  protected IConfigTask visitFlush(Flush node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
    return new FlushTask(((FlushStatement) node.getInnerTreeStatement()));
  }

  @Override
  protected IConfigTask visitSetConfiguration(SetConfiguration node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
    return new SetConfigurationTask(((SetConfigurationStatement) node.getInnerTreeStatement()));
  }

  private Optional<String> parseStringFromLiteralIfBinary(final Object value) {
    return value instanceof Literal && ((Literal) value).getTsValue() instanceof Binary
        ? Optional.of(
            ((Binary) ((Literal) value).getTsValue()).getStringValue(TSFileConfig.STRING_CHARSET))
        : Optional.empty();
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

  @Override
  protected IConfigTask visitCreatePipe(CreatePipe node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    for (String ExtractorAttribute : node.getExtractorAttributes().keySet()) {
      if (ExtractorAttribute.startsWith(SystemConstant.SYSTEM_PREFIX_KEY)) {
        throw new SemanticException(
            String.format(
                "Failed to create pipe %s, setting %s is not allowed.",
                node.getPipeName(), ExtractorAttribute));
      }
    }

    // Inject table model into the extractor attributes
    node.getExtractorAttributes()
        .put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);

    return new CreatePipeTask(node);
  }

  @Override
  protected IConfigTask visitAlterPipe(AlterPipe node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    for (String ExtractorAttribute : node.getExtractorAttributes().keySet()) {
      if (ExtractorAttribute.startsWith(SystemConstant.SYSTEM_PREFIX_KEY)) {
        throw new SemanticException(
            String.format(
                "Failed to alter pipe %s, modifying %s is not allowed.",
                node.getPipeName(), ExtractorAttribute));
      }
    }
    // If the source is replaced, sql-dialect uses the current Alter Pipe sql-dialect. If it is
    // modified, the original sql-dialect is used.
    if (node.isReplaceAllExtractorAttributes()) {
      node.getExtractorAttributes()
          .put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    }

    return new AlterPipeTask(node);
  }

  @Override
  protected IConfigTask visitDropPipe(DropPipe node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new DropPipeTask(node);
  }

  @Override
  protected IConfigTask visitStartPipe(StartPipe node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new StartPipeTask(node);
  }

  @Override
  protected IConfigTask visitStopPipe(StopPipe node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new StopPipeTask(node);
  }

  @Override
  protected IConfigTask visitShowPipes(ShowPipes node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowPipeTask(node);
  }

  @Override
  protected IConfigTask visitCreatePipePlugin(CreatePipePlugin node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new CreatePipePluginTask(node);
  }

  @Override
  protected IConfigTask visitDropPipePlugin(DropPipePlugin node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new DropPipePluginTask(node);
  }

  @Override
  protected IConfigTask visitShowPipePlugins(ShowPipePlugins node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowPipePluginsTask();
  }

  @Override
  protected IConfigTask visitCreateTopic(CreateTopic node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    // Inject table model into the topic attributes
    node.getTopicAttributes()
        .put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);

    return new CreateTopicTask(node);
  }

  @Override
  protected IConfigTask visitDropTopic(DropTopic node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new DropTopicTask(node);
  }

  @Override
  protected IConfigTask visitShowTopics(ShowTopics node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowTopicsTask(node);
  }

  @Override
  protected IConfigTask visitShowSubscriptions(ShowSubscriptions node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowSubscriptionsTask(node);
  }

  @Override
  protected IConfigTask visitShowCurrentUser(ShowCurrentUser node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowCurrentUserTask(context.getSession().getUserName());
  }

  @Override
  protected IConfigTask visitShowCurrentSqlDialect(
      ShowCurrentSqlDialect node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowCurrentSqlDialectTask(context.getSession().getSqlDialect().name());
  }

  @Override
  protected IConfigTask visitShowCurrentDatabase(
      ShowCurrentDatabase node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowCurrentDatabaseTask(context.getSession().getDatabaseName().orElse(null));
  }

  @Override
  protected IConfigTask visitShowVersion(ShowVersion node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowVersionTask();
  }

  @Override
  protected IConfigTask visitShowVariables(ShowVariables node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
    return new ShowVariablesTask();
  }

  @Override
  protected IConfigTask visitShowClusterId(ShowClusterId node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserHasMaintainPrivilege(context.getSession().getUserName());
    return new ShowClusterIdTask();
  }

  @Override
  protected IConfigTask visitShowCurrentTimestamp(
      ShowCurrentTimestamp node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowCurrentTimestampTask();
  }

  @Override
  protected IConfigTask visitKillQuery(KillQuery node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new KillQueryTask(node);
  }

  @Override
  protected IConfigTask visitCreateFunction(CreateFunction node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new CreateFunctionTask(node);
  }

  @Override
  protected IConfigTask visitShowFunctions(ShowFunctions node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowFunctionsTask(Model.TABLE);
  }

  @Override
  protected IConfigTask visitDropFunction(DropFunction node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new DropFunctionTask(Model.TABLE, node.getUdfName());
  }
}
