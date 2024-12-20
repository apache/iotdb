/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.InformationSchemaTable.INFORMATION_SCHEMA;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.QueryUtil.selectList;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.QueryUtil.simpleQuery;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.QueryUtil.table;

public final class ShowRewrite implements StatementRewrite.Rewrite {
  private final Metadata metadata;

  // private final SqlParser parser;
  // private final AccessControl accessControl;

  public ShowRewrite(Metadata metadata) {
    this.metadata = requireNonNull(metadata, "metadata is null");
    // this.parser = requireNonNull(parser, "parser is null");
    // this.accessControl = requireNonNull(accessControl, "accessControl is null");
  }

  @Override
  public Statement rewrite(
      StatementAnalyzerFactory analyzerFactory,
      SessionInfo session,
      Statement node,
      List<Expression> parameters,
      Map<NodeRef<Parameter>, Expression> parameterLookup,
      WarningCollector warningCollector) {
    Visitor visitor = new Visitor(metadata, session);
    return (Statement) visitor.process(node, null);
  }

  private static class Visitor extends AstVisitor<Node, Void> {
    private final Metadata metadata;
    private final SessionInfo session;

    public Visitor(Metadata metadata, SessionInfo session) {
      this.metadata = requireNonNull(metadata, "metadata is null");
      this.session = requireNonNull(session, "session is null");
    }

    @Override
    protected Node visitShowStatement(ShowStatement showStatement, Void context) {
      // CatalogSchemaName schema = createCatalogSchemaName(session, showQueries,
      // showQueries.getSchema());

      // accessControl.checkCanShowQueries(session.toSecurityContext(), schema);

      return simpleQuery(
          selectList(new AllColumns()),
          from(INFORMATION_SCHEMA, showStatement.getTableName()),
          showStatement.getWhere(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          showStatement.getOrderBy(),
          showStatement.getOffset(),
          showStatement.getLimit());
    }

    private static Relation from(String db, String table) {
      return table(QualifiedName.of(db, table));
    }

    @Override
    protected Node visitNode(Node node, Void context) {
      return node;
    }
  }
}
