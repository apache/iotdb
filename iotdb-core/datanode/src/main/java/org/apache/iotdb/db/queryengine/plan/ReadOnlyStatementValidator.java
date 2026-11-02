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

package org.apache.iotdb.db.queryengine.plan;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Deallocate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Execute;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExecuteImmediate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.KillQuery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Prepare;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowAIDevices;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowAINodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowAvailableUrls;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCluster;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowClusterId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowConfigNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCreateDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCreatePipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCreateTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentTimestamp;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDataNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowLoadedModels;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowModels;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipePlugins;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowRegions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTopics;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVariables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVersion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.DeviceSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.SeriesSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainAnalyzeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSqlDialectStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSystemStatusStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Checks SQL before analysis, which can itself create schema or allocate partitions. */
public final class ReadOnlyStatementValidator {

  private ReadOnlyStatementValidator() {}

  public static void check(final Statement statement) {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly() && !isAllowed(statement)) {
      reject();
    }
  }

  public static void check(
      final org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      final SqlParser parser,
      final IClientSession session) {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()
        && !isAllowed(statement, parser, session)) {
      reject();
    }
  }

  static boolean isAllowed(final Statement statement) {
    if (statement instanceof PipeEnrichedStatement) {
      return isAllowed(((PipeEnrichedStatement) statement).getInnerStatement());
    }
    if (statement instanceof QueryStatement) {
      return !((QueryStatement) statement).isSelectInto();
    }
    if (statement instanceof ExplainStatement) {
      return isAllowed(((ExplainStatement) statement).getQueryStatement());
    }
    if (statement instanceof ExplainAnalyzeStatement) {
      return isAllowed(((ExplainAnalyzeStatement) statement).getQueryStatement());
    }
    // Deleting a whole database and setting TTL are explicit disk-recovery exceptions.
    // They still use the normal metadata persistence and permission checks.
    if (statement instanceof DeleteDatabaseStatement
        || (statement instanceof SetTTLStatement && !(statement instanceof UnSetTTLStatement))
        || statement instanceof SetSystemStatusStatement
        || statement instanceof SetSqlDialectStatement
        || statement instanceof KillQueryStatement) {
      return true;
    }
    if (statement instanceof IConfigStatement) {
      return ((IConfigStatement) statement).getQueryType() == QueryType.READ;
    }
    // SELECT resolves schema through these internal reads on a schema-cache miss.
    return statement instanceof SeriesSchemaFetchStatement
        || statement instanceof DeviceSchemaFetchStatement
        || statement
            instanceof org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowStatement
        || statement.isQuery();
  }

  static boolean isAllowed(
      final org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      final SqlParser parser,
      final IClientSession session) {
    return isAllowed(statement, parser, session, new HashSet<>());
  }

  private static boolean isAllowed(
      final org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement statement,
      final SqlParser parser,
      final IClientSession session,
      final Set<String> preparedStatements) {
    if (statement instanceof WrappedStatement) {
      return isAllowed(((WrappedStatement) statement).getInnerTreeStatement());
    }
    if (statement instanceof Explain) {
      return isAllowed(((Explain) statement).getStatement(), parser, session, preparedStatements);
    }
    if (statement instanceof ExplainAnalyze) {
      return isAllowed(
          ((ExplainAnalyze) statement).getStatement(), parser, session, preparedStatements);
    }
    if (statement instanceof Execute) {
      final String name = ((Execute) statement).getStatementName().getValue();
      if (!preparedStatements.add(name)) {
        return false;
      }
      final var prepared = session.getPreparedStatement(name);
      // Let the normal execution path report an unknown prepared statement.
      return prepared == null || isAllowed(prepared.getSql(), parser, session, preparedStatements);
    }
    if (statement instanceof ExecuteImmediate) {
      return isAllowed(
          parser.createStatement(
              ((ExecuteImmediate) statement).getSqlString(), session.getZoneId(), session),
          parser,
          session,
          preparedStatements);
    }
    if (statement instanceof RelationalAuthorStatement) {
      return ((RelationalAuthorStatement) statement).getQueryType() == QueryType.READ;
    }
    if (statement instanceof AlterDB) {
      return onlySetsTTL(((AlterDB) statement).getProperties());
    }
    if (statement instanceof SetProperties) {
      return ((SetProperties) statement).getType() == SetProperties.Type.TABLE
          && onlySetsTTL(((SetProperties) statement).getProperties());
    }
    // Other SHOW forms wrap tree statements above or are rewritten to information_schema reads.
    // Session-local operations are safe and must remain usable while the server is read-only.
    // Table reads retain the attribute-sync registration in RegionWriteExecutor for cache
    // consistency.
    return statement instanceof Query
        || statement instanceof ShowStatement
        || statement instanceof CountStatement
        || statement instanceof ShowDB
        || statement instanceof ShowCreateDatabase
        || statement instanceof CountDB
        || statement instanceof ShowAIDevices
        || statement instanceof ShowAINodes
        || statement instanceof ShowAvailableUrls
        || statement instanceof ShowCluster
        || statement instanceof ShowClusterId
        || statement instanceof ShowConfigNodes
        || statement instanceof ShowCurrentDatabase
        || statement instanceof ShowCurrentSqlDialect
        || statement instanceof ShowCurrentTimestamp
        || statement instanceof ShowCurrentUser
        || statement instanceof ShowDataNodes
        || statement instanceof ShowFunctions
        || statement instanceof ShowIndex
        || statement instanceof ShowLoadedModels
        || statement instanceof ShowModels
        || statement instanceof ShowPipePlugins
        || statement instanceof ShowRegions
        || statement instanceof ShowVariables
        || statement instanceof ShowVersion
        || statement instanceof ShowTables
        || statement instanceof DescribeTable
        || statement instanceof ShowDevice
        || statement instanceof CountDevice
        || statement instanceof FetchDevice
        || statement instanceof ShowPipes
        || statement instanceof ShowCreatePipe
        || statement instanceof ShowTopics
        || statement instanceof ShowCreateTopic
        || statement instanceof ShowSubscriptions
        || statement instanceof DropDB
        || statement instanceof Use
        || statement instanceof KillQuery
        || statement instanceof SetSqlDialect
        || statement instanceof Prepare
        || statement instanceof Deallocate;
  }

  private static boolean onlySetsTTL(final List<Property> properties) {
    return !properties.isEmpty()
        && properties.stream()
            .allMatch(
                property ->
                    "ttl".equalsIgnoreCase(property.getName().getValue())
                        && !property.isSetToDefault());
  }

  private static void reject() {
    throw new IoTDBRuntimeException(
        CommonMessages.SYSTEM_READ_ONLY, TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), true);
  }
}
