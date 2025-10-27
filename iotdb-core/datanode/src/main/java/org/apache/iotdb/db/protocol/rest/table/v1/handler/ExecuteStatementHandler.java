/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.table.v1.handler;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowAIDevices;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowLoadedModels;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowModels;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipePlugins;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowRegions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTopics;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVariables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVersion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;

import java.util.HashSet;
import java.util.Set;

public class ExecuteStatementHandler {
  private ExecuteStatementHandler() {}

  private static final Set<Class<? extends Statement>> INVALID_STATEMENT_CLASSES = new HashSet<>();

  private static final Set<AuthorRType> INVALID_AUTHORTYPE = new HashSet<>();

  static {
    INVALID_STATEMENT_CLASSES.add(CountDevice.class);
    INVALID_STATEMENT_CLASSES.add(DescribeTable.class);
    INVALID_STATEMENT_CLASSES.add(Explain.class);
    INVALID_STATEMENT_CLASSES.add(ExplainAnalyze.class);
    INVALID_STATEMENT_CLASSES.add(Query.class);
    INVALID_STATEMENT_CLASSES.add(ShowAINodes.class);
    INVALID_STATEMENT_CLASSES.add(ShowCluster.class);
    INVALID_STATEMENT_CLASSES.add(ShowClusterId.class);
    INVALID_STATEMENT_CLASSES.add(ShowConfigNodes.class);
    INVALID_STATEMENT_CLASSES.add(ShowCurrentDatabase.class);
    INVALID_STATEMENT_CLASSES.add(ShowCurrentSqlDialect.class);
    INVALID_STATEMENT_CLASSES.add(ShowCurrentTimestamp.class);
    INVALID_STATEMENT_CLASSES.add(ShowCurrentUser.class);
    INVALID_STATEMENT_CLASSES.add(ShowDB.class);
    INVALID_STATEMENT_CLASSES.add(ShowDataNodes.class);
    INVALID_STATEMENT_CLASSES.add(ShowDevice.class);
    INVALID_STATEMENT_CLASSES.add(ShowFunctions.class);
    INVALID_STATEMENT_CLASSES.add(ShowIndex.class);
    INVALID_STATEMENT_CLASSES.add(ShowModels.class);
    INVALID_STATEMENT_CLASSES.add(ShowLoadedModels.class);
    INVALID_STATEMENT_CLASSES.add(ShowAIDevices.class);
    INVALID_STATEMENT_CLASSES.add(ShowPipePlugins.class);
    INVALID_STATEMENT_CLASSES.add(ShowPipes.class);
    INVALID_STATEMENT_CLASSES.add(ShowQueriesStatement.class);
    INVALID_STATEMENT_CLASSES.add(ShowSubscriptions.class);
    INVALID_STATEMENT_CLASSES.add(ShowRegions.class);
    INVALID_STATEMENT_CLASSES.add(ShowTables.class);
    INVALID_STATEMENT_CLASSES.add(ShowTopics.class);
    INVALID_STATEMENT_CLASSES.add(ShowVariables.class);
    INVALID_STATEMENT_CLASSES.add(ShowVersion.class);
    INVALID_STATEMENT_CLASSES.add(ShowStatement.class);
    INVALID_AUTHORTYPE.add(AuthorRType.LIST_USER);
    INVALID_AUTHORTYPE.add(AuthorRType.LIST_ROLE);
    INVALID_AUTHORTYPE.add(AuthorRType.LIST_USER_PRIV);
    INVALID_AUTHORTYPE.add(AuthorRType.LIST_ROLE_PRIV);
  }

  public static boolean validateStatement(Statement statement) {
    if (statement instanceof RelationalAuthorStatement
        && INVALID_AUTHORTYPE.contains(((RelationalAuthorStatement) statement).getAuthorType())) {
      return false;
    } else {
      return !INVALID_STATEMENT_CLASSES.contains(statement.getClass());
    }
  }
}
