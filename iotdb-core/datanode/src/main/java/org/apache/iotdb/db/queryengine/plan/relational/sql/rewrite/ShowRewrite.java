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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDiskUsageOfTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleGroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.commons.schema.table.InformationSchema.INFORMATION_DATABASE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.QueryUtil.selectList;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.QueryUtil.simpleQuery;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.QueryUtil.table;

public final class ShowRewrite implements StatementRewrite.Rewrite {

  @Override
  public Statement rewrite(
      final StatementAnalyzerFactory analyzerFactory,
      final SessionInfo session,
      final Statement node,
      final List<Expression> parameters,
      final Map<NodeRef<Parameter>, Expression> parameterLookup,
      final WarningCollector warningCollector,
      final MPPQueryContext queryContext) {
    final Visitor visitor = new Visitor();
    return (Statement) visitor.process(node, null);
  }

  private static class Visitor extends AstVisitor<Node, MPPQueryContext> {

    @Override
    protected Node visitShowQueriesStatement(ShowQueriesStatement node, MPPQueryContext context) {
      return visitShowStatement(node, context);
    }

    @Override
    protected Node visitShowDiskUsageOfTable(ShowDiskUsageOfTable node, MPPQueryContext context) {
      if (context != null) {
        context.setTimeOut(Long.MAX_VALUE);
      }
      return simpleQuery(
          selectList(
              new SingleColumn(new Identifier(ColumnHeaderConstant.NODE_ID_TABLE_MODEL)),
              new SingleColumn(
                  new FunctionCall(
                      QualifiedName.of(TableBuiltinAggregationFunction.SUM.getFunctionName()),
                      Collections.singletonList(
                          new Identifier(ColumnHeaderConstant.SIZE_IN_BYTES_TABLE_MODEL))),
                  new Identifier(ColumnHeaderConstant.SIZE_IN_BYTES_TABLE_MODEL))),
          from(INFORMATION_DATABASE, node.getTableName()),
          node.getWhere(),
          Optional.of(
              new GroupBy(
                  false,
                  Collections.singletonList(
                      new SimpleGroupBy(
                          Collections.singletonList(
                              new Identifier(ColumnHeaderConstant.NODE_ID_TABLE_MODEL)))))),
          Optional.empty(),
          Optional.empty(),
          node.getOrderBy(),
          node.getOffset(),
          node.getLimit());
    }

    @Override
    protected Node visitShowStatement(
        final ShowStatement showStatement, final MPPQueryContext context) {
      return simpleQuery(
          selectList(new AllColumns()),
          from(INFORMATION_DATABASE, showStatement.getTableName()),
          showStatement.getWhere(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          showStatement.getOrderBy(),
          showStatement.getOffset(),
          showStatement.getLimit());
    }

    @Override
    protected Node visitCountStatement(
        final CountStatement countStatement, final MPPQueryContext context) {
      return simpleQuery(
          new Select(
              false,
              Collections.singletonList(
                  new SingleColumn(
                      new FunctionCall(
                          QualifiedName.of(Collections.singletonList(new Identifier("count"))),
                          Collections.emptyList()),
                      new Identifier("count(devices)")))),
          from(INFORMATION_DATABASE, countStatement.getTableName()),
          countStatement.getWhere(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());
    }

    private static Relation from(final String db, final String table) {
      return table(QualifiedName.of(db, table));
    }

    @Override
    protected Node visitNode(final Node node, final MPPQueryContext context) {
      return node;
    }
  }
}
