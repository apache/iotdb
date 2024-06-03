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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Statement;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Analyzer {

  private final StatementAnalyzerFactory statementAnalyzerFactory;

  private final SessionInfo session;
  private final List<Expression> parameters;

  private final Map<NodeRef<Parameter>, Expression> parameterLookup;

  private final WarningCollector warningCollector;

  public Analyzer(
      SessionInfo session,
      StatementAnalyzerFactory statementAnalyzerFactory,
      List<Expression> parameters,
      Map<NodeRef<Parameter>, Expression> parameterLookup,
      WarningCollector warningCollector) {
    this.session = requireNonNull(session, "session is null");
    this.statementAnalyzerFactory =
        requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
    this.parameters = parameters;
    this.parameterLookup = parameterLookup;
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
  }

  public Analysis analyze(Statement statement) {
    Analysis analysis = new Analysis(statement, parameterLookup);
    StatementAnalyzer analyzer =
        statementAnalyzerFactory.createStatementAnalyzer(
            analysis, session, warningCollector, CorrelationSupport.ALLOWED);

    analyzer.analyze(statement);

    // TODO access control
    // check column access permissions for each table
    //    analysis.getTableColumnReferences().forEach((accessControlInfo, tableColumnReferences) ->
    //        tableColumnReferences.forEach((tableName, columns) ->
    //            accessControlInfo.getAccessControl().checkCanSelectFromColumns(
    //                accessControlInfo.getSecurityContext(session.getRequiredTransactionId(),
    // session.getQueryId(),
    //                    session.getStart()),
    //                tableName,
    //                columns)));

    return analysis;
  }
}
