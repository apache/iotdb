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
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
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
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.RemoveConfigNodeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.RemoveDataNodeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterIdTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowFunctionsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowPipePluginsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowVariablesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ai.CreateTrainingTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ai.ShowModelsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.ExtendRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.MigrateRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.ReconstructRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.RemoveRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableAddColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableCommentColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableCommentTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableDropColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableRenameColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableRenameTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableSetPropertiesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ClearCacheTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateTableViewTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DeleteDeviceTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DescribeTableDetailsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DescribeTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DropDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.DropTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.RelationalAuthorizerTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowAINodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowConfigNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowCreateTableTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowCreateViewTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowDataNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowTablesDetailsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowTablesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.UseDBTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.SetSqlDialectTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentDatabaseTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentSqlDialectTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentTimestampTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentUserTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowVersionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.FlushTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.KillQueryTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.LoadConfigurationTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.SetConfigurationTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.SetSystemStatusTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.StartRepairDataTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.StopRepairDataTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.AlterPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.CreatePipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.DropPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.ShowPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.StartPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.StopPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.CreateTopicTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.DropSubscriptionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.DropTopicTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.ShowSubscriptionsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.ShowTopicsTask;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analyzer;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTraining;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateView;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DeleteDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropSubscription;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExtendRegion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Flush;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.KillQuery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadConfiguration;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.MigrateRegion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ReconstructRegion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RemoveConfigNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RemoveDataNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RemoveRegion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetColumnComment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetConfiguration;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetSystemStatus;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetTableComment;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowModels;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipePlugins;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowRegions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTopics;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVariables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVersion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartRepairData;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopRepairData;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ViewFieldDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite.StatementRewrite;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveConfigNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveDataNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.LoadConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSystemStatusStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.StartRepairDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.StopRepairDataStatement;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MAX_DATABASE_NAME_LENGTH;
import static org.apache.iotdb.commons.conf.IoTDBConstant.TTL_INFINITE;
import static org.apache.iotdb.commons.executable.ExecutableManager.getUnTrustedUriErrorMsg;
import static org.apache.iotdb.commons.executable.ExecutableManager.isUriTrusted;
import static org.apache.iotdb.commons.schema.table.TsTable.TABLE_ALLOWED_PROPERTIES;
import static org.apache.iotdb.commons.schema.table.TsTable.TIME_COLUMN_NAME;
import static org.apache.iotdb.commons.schema.table.TsTable.TTL_PROPERTY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.DATA_REGION_GROUP_NUM_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.SCHEMA_REGION_GROUP_NUM_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.TIME_PARTITION_INTERVAL_KEY;
import static org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask.TTL_KEY;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

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
    accessControl.checkCanCreateDatabase(context.getSession().getUserName(), node.getDbName());
    return visitDatabaseStatement(node, context);
  }

  @Override
  protected IConfigTask visitAlterDB(final AlterDB node, final MPPQueryContext context) {
    accessControl.checkCanAlterDatabase(context.getSession().getUserName(), node.getDbName());
    return visitDatabaseStatement(node, context);
  }

  private IConfigTask visitDatabaseStatement(
      final DatabaseStatement node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    final TDatabaseSchema schema = new TDatabaseSchema();
    schema.setIsTableModel(true);

    final String dbName = node.getDbName();
    validateDatabaseName(dbName);

    schema.setName(dbName);

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
    return new DropDBTask(node, clientSession);
  }

  @Override
  protected IConfigTask visitShowDB(final ShowDB node, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowDBTask(
        node,
        databaseName -> canShowDB(accessControl, context.getSession().getUserName(), databaseName));
  }

  public static boolean canShowDB(
      final AccessControl accessControl, final String userName, final String databaseName) {
    try {
      accessControl.checkCanShowOrUseDatabase(userName, databaseName);
      return true;
    } catch (final AccessDeniedException e) {
      return false;
    }
  }

  @Override
  protected IConfigTask visitShowCluster(
      final ShowCluster showCluster, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
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
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    final ShowRegionStatement treeStatement = new ShowRegionStatement();
    treeStatement.setRegionType(showRegions.getRegionType());
    treeStatement.setDatabases(
        Objects.nonNull(showRegions.getDatabase())
            ? Collections.singletonList(new PartialPath(new String[] {showRegions.getDatabase()}))
            : null);
    treeStatement.setNodeIds(showRegions.getNodeIds());
    return new ShowRegionTask(treeStatement, true);
  }

  @Override
  protected IConfigTask visitRemoveDataNode(
      final RemoveDataNode removeDataNode, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    final RemoveDataNodeStatement treeStatement =
        new RemoveDataNodeStatement(removeDataNode.getNodeIds());
    return new RemoveDataNodeTask(treeStatement);
  }

  @Override
  protected IConfigTask visitRemoveConfigNode(
      final RemoveConfigNode removeConfigNode, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    final RemoveConfigNodeStatement treeStatement =
        new RemoveConfigNodeStatement(removeConfigNode.getNodeId());
    return new RemoveConfigNodeTask(treeStatement);
  }

  @Override
  protected IConfigTask visitShowDataNodes(
      final ShowDataNodes showDataNodesStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new ShowDataNodesTask();
  }

  @Override
  protected IConfigTask visitShowConfigNodes(
      final ShowConfigNodes showConfigNodesStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new ShowConfigNodesTask();
  }

  @Override
  protected IConfigTask visitShowAINodes(
      final ShowAINodes showAINodesStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new ShowAINodesTask();
  }

  @Override
  protected IConfigTask visitClearCache(
      final ClearCache clearCacheStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new ClearCacheTask(clearCacheStatement);
  }

  @Override
  protected IConfigTask visitCreateTable(final CreateTable node, final MPPQueryContext context) {
    final Pair<String, TsTable> databaseTablePair = parseTable4CreateTableOrView(node, context);
    return new CreateTableTask(
        databaseTablePair.getRight(), databaseTablePair.getLeft(), node.isIfNotExists());
  }

  @Override
  protected IConfigTask visitCreateView(final CreateView node, final MPPQueryContext context) {
    final Pair<String, TsTable> databaseTablePair = parseTable4CreateTableOrView(node, context);
    final TsTable table = databaseTablePair.getRight();
    accessControl.checkCanCreateViewFromTreePath(
        context.getSession().getUserName(), node.getPrefixPath());
    final String msg = TreeViewSchema.setPathPattern(table, node.getPrefixPath());
    if (Objects.nonNull(msg)) {
      throw new SemanticException(msg);
    }
    if (node.isRestrict()) {
      TreeViewSchema.setRestrict(table);
    }
    return new CreateTableViewTask(
        databaseTablePair.getRight(), databaseTablePair.getLeft(), node.isReplace());
  }

  private Pair<String, TsTable> parseTable4CreateTableOrView(
      final CreateTable node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getName());
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanCreateTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    final TsTable table = new TsTable(tableName);

    table.setProps(convertPropertiesToMap(node.getProperties(), false));
    if (Objects.nonNull(node.getComment())) {
      table.addProp(TsTable.COMMENT_KEY, node.getComment());
    }

    // TODO: Place the check at statement analyzer
    boolean hasTimeColumn = false;
    final Set<String> sourceNameSet = new HashSet<>();
    for (final ColumnDefinition columnDefinition : node.getElements()) {
      final TsTableColumnCategory category = columnDefinition.getColumnCategory();
      final String columnName = columnDefinition.getName().getValue();
      final TSDataType dataType = getDataType(columnDefinition.getType());
      final String comment = columnDefinition.getComment();
      if (checkTimeColumnIdempotent(category, columnName, dataType, comment, table)
          && !hasTimeColumn) {
        hasTimeColumn = true;
        continue;
      }
      if (table.getColumnSchema(columnName) != null) {
        throw new SemanticException(
            String.format("Columns in table shall not share the same name %s.", columnName));
      }
      final TsTableColumnSchema schema =
          TableHeaderSchemaValidator.generateColumnSchema(
              category,
              columnName,
              dataType,
              comment,
              columnDefinition instanceof ViewFieldDefinition
                      && Objects.nonNull(((ViewFieldDefinition) columnDefinition).getFrom())
                  ? ((ViewFieldDefinition) columnDefinition).getFrom().getValue()
                  : null);
      if (!sourceNameSet.add(TreeViewSchema.getSourceName(schema))) {
        throw new SemanticException(
            String.format(
                "The duplicated source measurement %s is unsupported yet.",
                TreeViewSchema.getSourceName(schema)));
      }
      table.addColumnSchema(schema);
    }
    return new Pair<>(database, table);
  }

  private boolean checkTimeColumnIdempotent(
      final TsTableColumnCategory category,
      final String columnName,
      final TSDataType dataType,
      final String comment,
      final TsTable table) {
    if (category == TsTableColumnCategory.TIME || columnName.equals(TIME_COLUMN_NAME)) {
      if (category == TsTableColumnCategory.TIME
          && columnName.equals(TIME_COLUMN_NAME)
          && dataType == TSDataType.TIMESTAMP) {
        if (Objects.nonNull(comment)) {
          final TsTableColumnSchema columnSchema =
              new TimeColumnSchema(TIME_COLUMN_NAME, TSDataType.TIMESTAMP);
          columnSchema.getProps().put(TsTable.COMMENT_KEY, comment);
          table.addColumnSchema(columnSchema);
        }
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
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanAlterTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    final String newName = node.getTarget().getValue();
    if (tableName.equals(newName)) {
      throw new SemanticException("The table's old name shall not be equal to the new one.");
    }

    return new AlterTableRenameTableTask(
        database,
        tableName,
        context.getQueryId().getId(),
        node.getTarget().getValue(),
        node.tableIfExists(),
        node.isView());
  }

  @Override
  protected IConfigTask visitAddColumn(final AddColumn node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTableName());
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanAlterTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    final ColumnDefinition definition = node.getColumn();
    return new AlterTableAddColumnTask(
        database,
        tableName,
        Collections.singletonList(
            TableHeaderSchemaValidator.generateColumnSchema(
                definition.getColumnCategory(),
                definition.getName().getValue(),
                getDataType(definition.getType()),
                definition.getComment(),
                definition instanceof ViewFieldDefinition
                        && Objects.nonNull(((ViewFieldDefinition) definition).getFrom())
                    ? ((ViewFieldDefinition) definition).getFrom().getValue()
                    : null)),
        context.getQueryId().getId(),
        node.tableIfExists(),
        node.columnIfNotExists(),
        node.isView());
  }

  @Override
  protected IConfigTask visitRenameColumn(final RenameColumn node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTable());
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanAlterTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    final String oldName = node.getSource().getValue();
    final String newName = node.getTarget().getValue();
    if (oldName.equals(newName)) {
      throw new SemanticException("The column's old name shall not be equal to the new one.");
    }

    return new AlterTableRenameColumnTask(
        database,
        tableName,
        node.getSource().getValue(),
        node.getTarget().getValue(),
        context.getQueryId().getId(),
        node.tableIfExists(),
        node.columnIfExists(),
        node.isView());
  }

  @Override
  protected IConfigTask visitDropColumn(final DropColumn node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTable());
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanAlterTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    return new AlterTableDropColumnTask(
        database,
        tableName,
        node.getField().getValue(),
        context.getQueryId().getId(),
        node.tableIfExists(),
        node.columnIfExists(),
        node.isView());
  }

  @Override
  protected IConfigTask visitSetProperties(
      final SetProperties node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getName());
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanAlterTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    return new AlterTableSetPropertiesTask(
        database,
        tableName,
        convertPropertiesToMap(node.getProperties(), true),
        context.getQueryId().getId(),
        node.ifExists(),
        node.getType() == SetProperties.Type.TREE_VIEW);
  }

  @Override
  protected IConfigTask visitSetTableComment(
      final SetTableComment node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTableName());
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanAlterTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    return new AlterTableCommentTableTask(
        database,
        tableName,
        context.getQueryId().getId(),
        node.ifExists(),
        node.getComment(),
        node.isView());
  }

  @Override
  protected IConfigTask visitSetColumnComment(
      final SetColumnComment node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTable());
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanAlterTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    return new AlterTableCommentColumnTask(
        database,
        tableName,
        node.getField().getValue(),
        context.getQueryId().getId(),
        node.tableIfExists(),
        node.columnIfExists(),
        node.getComment());
  }

  public static void validateDatabaseName(final String dbName) throws SemanticException {
    // Check database length here
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
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanDropTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    return new DropTableTask(
        database, tableName, context.getQueryId().getId(), node.isExists(), node.isView());
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

    accessControl.checkCanDeleteFromTable(
        context.getSession().getUserName(),
        new QualifiedObjectName(node.getDatabase(), node.getTableName()));
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
    String finalDatabase = database;
    final Predicate<String> checkCanShowTable =
        tableName ->
            canShowTable(
                accessControl, context.getSession().getUserName(), finalDatabase, tableName);
    return node.isDetails()
        ? new ShowTablesDetailsTask(database, checkCanShowTable)
        : new ShowTablesTask(database, checkCanShowTable);
  }

  public static boolean canShowTable(
      final AccessControl accessControl,
      final String userName,
      final String databaseName,
      final String tableName) {
    try {
      accessControl.checkCanShowOrDescTable(
          userName, new QualifiedObjectName(databaseName, tableName));
      return true;
    } catch (final AccessDeniedException e) {
      return false;
    }
  }

  @Override
  protected IConfigTask visitDescribeTable(
      final DescribeTable node, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    final Pair<String, String> databaseTablePair = splitQualifiedName(node.getTable());
    final String database = databaseTablePair.getLeft();
    final String tableName = databaseTablePair.getRight();

    accessControl.checkCanShowOrDescTable(
        context.getSession().getUserName(), new QualifiedObjectName(database, tableName));

    if (Boolean.TRUE.equals(node.getShowCreateView())) {
      return new ShowCreateViewTask(database, tableName);
    } else if (Boolean.FALSE.equals(node.getShowCreateView())) {
      return new ShowCreateTableTask(database, tableName);
    }
    return node.isDetails()
        ? new DescribeTableDetailsTask(database, tableName)
        : new DescribeTableTask(database, tableName);
  }

  @Override
  protected IConfigTask visitFlush(final Flush node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new FlushTask(((FlushStatement) node.getInnerTreeStatement()));
  }

  @Override
  protected IConfigTask visitSetConfiguration(SetConfiguration node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new SetConfigurationTask(((SetConfigurationStatement) node.getInnerTreeStatement()));
  }

  @Override
  protected IConfigTask visitStartRepairData(StartRepairData node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new StartRepairDataTask(((StartRepairDataStatement) node.getInnerTreeStatement()));
  }

  @Override
  protected IConfigTask visitStopRepairData(StopRepairData node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new StopRepairDataTask(((StopRepairDataStatement) node.getInnerTreeStatement()));
  }

  @Override
  protected IConfigTask visitLoadConfiguration(LoadConfiguration node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new LoadConfigurationTask(((LoadConfigurationStatement) node.getInnerTreeStatement()));
  }

  @Override
  protected IConfigTask visitSetSystemStatus(SetSystemStatus node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new SetSystemStatusTask(((SetSystemStatusStatement) node.getInnerTreeStatement()));
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
  protected IConfigTask visitCreatePipe(final CreatePipe node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final String userName = context.getSession().getUserName();
    accessControl.checkUserIsAdmin(userName);

    final Map<String, String> extractorAttributes = node.getExtractorAttributes();
    final String pipeName = node.getPipeName();
    for (final String ExtractorAttribute : extractorAttributes.keySet()) {
      if (ExtractorAttribute.startsWith(SystemConstant.SYSTEM_PREFIX_KEY)) {
        throw new SemanticException(
            String.format(
                "Failed to create pipe %s, setting %s is not allowed.",
                node.getPipeName(), ExtractorAttribute));
      }
    }

    // Inject table model into the extractor attributes
    extractorAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    checkAndEnrichSourceUserName(pipeName, extractorAttributes, userName, false);
    checkAndEnrichSinkUserName(pipeName, node.getConnectorAttributes(), userName, false);

    mayChangeSourcePattern(extractorAttributes);

    return new CreatePipeTask(node);
  }

  public static void checkAndEnrichSourceUserName(
      final String pipeName,
      final Map<String, String> replacedExtractorAttributes,
      final String userName,
      final boolean isAlter) {
    final PipeParameters extractorParameters = new PipeParameters(replacedExtractorAttributes);
    final String pluginName =
        extractorParameters
            .getStringOrDefault(
                Arrays.asList(
                    PipeExtractorConstant.EXTRACTOR_KEY, PipeExtractorConstant.SOURCE_KEY),
                BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
            .toLowerCase();

    if (!pluginName.equals(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
        && !pluginName.equals(BuiltinPipePlugin.IOTDB_SOURCE.getPipePluginName())) {
      return;
    }

    if (!extractorParameters.hasAnyAttributes(
        PipeExtractorConstant.EXTRACTOR_IOTDB_USER_KEY,
        PipeExtractorConstant.SOURCE_IOTDB_USER_KEY,
        PipeExtractorConstant.EXTRACTOR_IOTDB_USERNAME_KEY,
        PipeExtractorConstant.SOURCE_IOTDB_USERNAME_KEY)) {
      replacedExtractorAttributes.put(PipeExtractorConstant.SOURCE_IOTDB_USERNAME_KEY, userName);
    } else if (!extractorParameters.hasAnyAttributes(
        PipeExtractorConstant.EXTRACTOR_IOTDB_PASSWORD_KEY,
        PipeExtractorConstant.SOURCE_IOTDB_PASSWORD_KEY)) {
      throw new SemanticException(
          String.format(
              "Failed to %s pipe %s, in iotdb-source, password must be set when the username is specified.",
              isAlter ? "alter" : "create", pipeName));
    }
  }

  private static void mayChangeSourcePattern(final Map<String, String> extractorAttributes) {
    final PipeParameters extractorParameters = new PipeParameters(extractorAttributes);

    final String pluginName =
        extractorParameters
            .getStringOrDefault(
                Arrays.asList(
                    PipeExtractorConstant.EXTRACTOR_KEY, PipeExtractorConstant.SOURCE_KEY),
                BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
            .toLowerCase();

    if (!pluginName.equals(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
        && !pluginName.equals(BuiltinPipePlugin.IOTDB_SOURCE.getPipePluginName())) {
      return;
    }

    // Use lower case because database + table name are all in lower cases
    extractorParameters.computeAttributeIfExists(
        (k, v) -> v.toLowerCase(Locale.ENGLISH),
        PipeExtractorConstant.EXTRACTOR_DATABASE_KEY,
        PipeExtractorConstant.SOURCE_DATABASE_KEY,
        PipeExtractorConstant.EXTRACTOR_DATABASE_NAME_KEY,
        PipeExtractorConstant.SOURCE_DATABASE_NAME_KEY);

    extractorParameters.computeAttributeIfExists(
        (k, v) -> v.toLowerCase(Locale.ENGLISH),
        PipeExtractorConstant.EXTRACTOR_TABLE_KEY,
        PipeExtractorConstant.SOURCE_TABLE_KEY,
        PipeExtractorConstant.EXTRACTOR_TABLE_NAME_KEY,
        PipeExtractorConstant.SOURCE_TABLE_NAME_KEY);
  }

  public static void checkAndEnrichSinkUserName(
      final String pipeName,
      final Map<String, String> connectorAttributes,
      final String userName,
      final boolean isAlter) {
    final PipeParameters connectorParameters = new PipeParameters(connectorAttributes);
    final String pluginName =
        connectorParameters
            .getStringOrDefault(
                Arrays.asList(PipeConnectorConstant.CONNECTOR_KEY, PipeConnectorConstant.SINK_KEY),
                BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName())
            .toLowerCase();

    if (!pluginName.equals(BuiltinPipePlugin.WRITE_BACK_CONNECTOR.getPipePluginName())
        && !pluginName.equals(BuiltinPipePlugin.WRITE_BACK_SINK.getPipePluginName())) {
      return;
    }

    if (!connectorParameters.hasAnyAttributes(
        PipeConnectorConstant.CONNECTOR_IOTDB_USER_KEY,
        PipeConnectorConstant.SINK_IOTDB_USER_KEY,
        PipeConnectorConstant.CONNECTOR_IOTDB_USERNAME_KEY,
        PipeConnectorConstant.SINK_IOTDB_USERNAME_KEY)) {
      connectorAttributes.put(PipeConnectorConstant.SINK_IOTDB_USERNAME_KEY, userName);
    } else if (!connectorParameters.hasAnyAttributes(
        PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_KEY,
        PipeConnectorConstant.SINK_IOTDB_PASSWORD_KEY)) {
      throw new SemanticException(
          String.format(
              "Failed to %s pipe %s, in write-back-sink, password must be set when the username is specified.",
              isAlter ? "alter" : "create", pipeName));
    }
  }

  @Override
  protected IConfigTask visitAlterPipe(final AlterPipe node, final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    final String userName = context.getSession().getUserName();
    accessControl.checkUserIsAdmin(userName);

    final String pipeName = node.getPipeName();
    final Map<String, String> extractorAttributes = node.getExtractorAttributes();
    for (final String extractorAttributeKey : extractorAttributes.keySet()) {
      if (extractorAttributeKey.startsWith(SystemConstant.SYSTEM_PREFIX_KEY)) {
        throw new SemanticException(
            String.format(
                "Failed to alter pipe %s, modifying %s is not allowed.",
                pipeName, extractorAttributeKey));
      }
    }
    // If the source is replaced, sql-dialect uses the current Alter Pipe sql-dialect. If it is
    // modified, the original sql-dialect is used.
    if (node.isReplaceAllExtractorAttributes()) {
      extractorAttributes.put(
          SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
      checkAndEnrichSourceUserName(pipeName, extractorAttributes, userName, true);
    }
    mayChangeSourcePattern(extractorAttributes);

    if (node.isReplaceAllConnectorAttributes()) {
      checkAndEnrichSinkUserName(pipeName, node.getConnectorAttributes(), userName, true);
    }

    return new AlterPipeTask(node, userName);
  }

  @Override
  protected IConfigTask visitDropPipe(DropPipe node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new DropPipeTask(node);
  }

  @Override
  protected IConfigTask visitStartPipe(StartPipe node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new StartPipeTask(node);
  }

  @Override
  protected IConfigTask visitStopPipe(StopPipe node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new StopPipeTask(node);
  }

  @Override
  protected IConfigTask visitShowPipes(ShowPipes node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new ShowPipeTask(node);
  }

  @Override
  protected IConfigTask visitCreatePipePlugin(CreatePipePlugin node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    if (node.getUriString() != null && isUriTrusted(node.getUriString())) {
      // 1. user specified uri and that uri is trusted
      // 2. user doesn't specify uri
      return new CreatePipePluginTask(node);
    } else {
      // user specified uri and that uri is not trusted
      throw new SemanticException(getUnTrustedUriErrorMsg(node.getUriString()));
    }
  }

  @Override
  protected IConfigTask visitDropPipePlugin(DropPipePlugin node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new DropPipePluginTask(node);
  }

  @Override
  protected IConfigTask visitShowPipePlugins(ShowPipePlugins node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowPipePluginsTask(node);
  }

  @Override
  protected IConfigTask visitCreateTopic(CreateTopic node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());

    // Inject table model into the topic attributes
    node.getTopicAttributes()
        .put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);

    return new CreateTopicTask(node);
  }

  @Override
  protected IConfigTask visitDropTopic(DropTopic node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new DropTopicTask(node);
  }

  @Override
  protected IConfigTask visitShowTopics(ShowTopics node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new ShowTopicsTask(node);
  }

  @Override
  protected IConfigTask visitShowSubscriptions(ShowSubscriptions node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new ShowSubscriptionsTask(node);
  }

  @Override
  protected IConfigTask visitDropSubscription(DropSubscription node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new DropSubscriptionTask(node);
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
  protected IConfigTask visitSetSqlDialect(SetSqlDialect node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    return new SetSqlDialectTask(node.getSqlDialect());
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
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new ShowVariablesTask();
  }

  @Override
  protected IConfigTask visitShowClusterId(ShowClusterId node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new ShowClusterIdTask();
  }

  @Override
  protected IConfigTask visitShowCurrentTimestamp(
      ShowCurrentTimestamp node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowCurrentTimestampTask();
  }

  @Override
  protected IConfigTask visitRelationalAuthorPlan(
      RelationalAuthorStatement node, MPPQueryContext context) {
    accessControl.checkUserCanRunRelationalAuthorStatement(
        context.getSession().getUserName(), node);
    context.setQueryType(node.getQueryType());
    return new RelationalAuthorizerTask(node);
  }

  @Override
  protected IConfigTask visitKillQuery(KillQuery node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new KillQueryTask(node);
  }

  @Override
  protected IConfigTask visitCreateFunction(CreateFunction node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    if (node.getUriString().map(ExecutableManager::isUriTrusted).orElse(true)) {
      // 1. user specified uri and that uri is trusted
      // 2. user doesn't specify uri
      return new CreateFunctionTask(node);
    } else {
      // user specified uri and that uri is not trusted
      throw new SemanticException(getUnTrustedUriErrorMsg(node.getUriString().get()));
    }
  }

  @Override
  protected IConfigTask visitShowFunctions(ShowFunctions node, MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    return new ShowFunctionsTask(Model.TABLE);
  }

  @Override
  protected IConfigTask visitDropFunction(DropFunction node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    return new DropFunctionTask(Model.TABLE, node.getUdfName());
  }

  @Override
  protected IConfigTask visitMigrateRegion(MigrateRegion migrateRegion, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    return new MigrateRegionTask(migrateRegion);
  }

  @Override
  protected IConfigTask visitReconstructRegion(
      ReconstructRegion reconstructRegion, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    return new ReconstructRegionTask(reconstructRegion);
  }

  @Override
  protected IConfigTask visitExtendRegion(ExtendRegion extendRegion, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    return new ExtendRegionTask(extendRegion);
  }

  @Override
  protected IConfigTask visitRemoveRegion(RemoveRegion removeRegion, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    accessControl.checkUserIsAdmin(context.getSession().getUserName());
    // As the implementation is identical, we'll simply translate to the
    // corresponding tree-model variant and execute that.
    return new RemoveRegionTask(removeRegion);
  }

  @Override
  protected IConfigTask visitCreateTraining(CreateTraining node, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    return new CreateTrainingTask(
        node.getModelId(), node.getParameters(), node.getExistingModelId(), node.getTargetSql());
  }

  @Override
  protected IConfigTask visitShowModels(ShowModels node, MPPQueryContext context) {
    return new ShowModelsTask(node.getModelId());
  }
}
