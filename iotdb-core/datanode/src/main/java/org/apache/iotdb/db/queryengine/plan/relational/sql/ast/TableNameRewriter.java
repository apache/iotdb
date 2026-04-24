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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import java.util.Optional;

public class TableNameRewriter extends AstVisitor<Node, String> {

  /**
   * Normalize all table names in the Statement to dbName.tableName
   *
   * @param statement original AST
   * @param defaultDb The dbName currently used in the session (use dbName;)
   * @return normalized Statement
   */
  public static Statement rewrite(Statement statement, String defaultDb) {
    return (Statement) statement.accept(new TableNameRewriter(), defaultDb);
  }

  @Override
  protected Node visitNode(Node node, String defaultDb) {
    return node;
  }

  @Override
  protected Node visitQuery(Query node, String defaultDb) {
    // rewrite queryBody
    QueryBody newBody = (QueryBody) node.getQueryBody().accept(this, defaultDb);

    return new Query(
        node.getLocation().orElse(null),
        node.getWith(),
        newBody,
        node.getFill(),
        node.getOrderBy(),
        node.getOffset(),
        node.getLimit());
  }

  @Override
  protected Node visitQuerySpecification(QuerySpecification node, String defaultDb) {
    // rewrite FROM
    Optional<Relation> newFrom =
        node.getFrom().map(relation -> (Relation) relation.accept(this, defaultDb));

    // rewrite WHERE
    Optional<Expression> newWhere =
        node.getWhere().map(expr -> (Expression) expr.accept(this, defaultDb));

    return new QuerySpecification(
        node.getLocation().orElse(null),
        node.getSelect(),
        newFrom,
        newWhere,
        node.getGroupBy(),
        node.getHaving(),
        node.getFill(),
        node.getWindows(),
        node.getOrderBy(),
        node.getOffset(),
        node.getLimit());
  }

  @Override
  protected Node visitTable(Table node, String defaultDb) {
    QualifiedName name = node.getName();
    if (name.getParts().size() == 1 && defaultDb != null) {
      // tableName -> dbName.tableName
      return new Table(QualifiedName.of(defaultDb, name.getSuffix()));
    }
    return node;
  }

  @Override
  protected Node visitJoin(Join node, String defaultDb) {
    Relation newLeft = (Relation) node.getLeft().accept(this, defaultDb);
    Relation newRight = (Relation) node.getRight().accept(this, defaultDb);

    if (node.getCriteria().isPresent()) {
      return new Join(
          node.getLocation().orElse(null),
          node.getType(),
          newLeft,
          newRight,
          node.getCriteria().get());
    } else {
      return new Join(node.getLocation().orElse(null), node.getType(), newLeft, newRight);
    }
  }

  @Override
  protected Node visitAliasedRelation(AliasedRelation node, String defaultDb) {
    Relation rewritten = (Relation) node.getRelation().accept(this, defaultDb);
    return new AliasedRelation(rewritten, node.getAlias(), node.getColumnNames());
  }
}
