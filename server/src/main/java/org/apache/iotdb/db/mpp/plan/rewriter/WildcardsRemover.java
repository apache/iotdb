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

package org.apache.iotdb.db.mpp.plan.rewriter;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.sql.SQLParserException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.filter.BasicFunctionFilter;
import org.apache.iotdb.db.mpp.common.filter.FunctionFilter;
import org.apache.iotdb.db.mpp.common.filter.InFilter;
import org.apache.iotdb.db.mpp.common.filter.LikeFilter;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.common.filter.RegexpFilter;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.ColumnPaginationController;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.constant.FilterConstant;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.plan.statement.crud.LastQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.qp.constant.SQLConstant;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This rewriter:
 *
 * <p>1. Bind metadata to paths in SELECT, WHERE, and WITHOUT NULL clauses.
 *
 * <p>2. Remove wildcards and apply SLIMIT & SOFFSET.
 */
public class WildcardsRemover {

  private SchemaTree schemaTree;
  private TypeProvider typeProvider;

  private ColumnPaginationController paginationController;

  public Statement rewrite(Statement statement, TypeProvider typeProvider, SchemaTree schemaTree)
      throws StatementAnalyzeException, PathNumOverLimitException {
    QueryStatement queryStatement = (QueryStatement) statement;
    this.paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(),
            queryStatement.getSeriesOffset(),
            queryStatement.isAlignByDevice()
                || queryStatement.disableAlign()
                || queryStatement instanceof LastQueryStatement
                || queryStatement.isGroupByLevel());
    this.schemaTree = schemaTree;
    this.typeProvider = typeProvider;

    // remove wildcards in WHERE clause
    if (queryStatement.getWhereCondition() != null) {
      removeWildcardsInQueryFilter(queryStatement);
    }

    return queryStatement;
  }

  private void removeWildcardsInQueryFilter(QueryStatement queryStatement)
      throws StatementAnalyzeException {
    WhereCondition whereCondition = queryStatement.getWhereCondition();
    List<PartialPath> fromPaths = queryStatement.getFromComponent().getPrefixPaths();

    Set<PartialPath> resultPaths = new HashSet<>();
    //    whereCondition.setQueryFilter(
    //        removeWildcardsInQueryFilter(whereCondition.getQueryFilter(), fromPaths,
    // resultPaths));
    //    whereCondition.getQueryFilter().setPathSet(resultPaths);
  }

  private QueryFilter removeWildcardsInQueryFilter(
      QueryFilter filter, List<PartialPath> fromPaths, Set<PartialPath> resultPaths)
      throws StatementAnalyzeException {
    if (!filter.isLeaf()) {
      List<QueryFilter> newFilterList = new ArrayList<>();
      for (QueryFilter child : filter.getChildren()) {
        newFilterList.add(removeWildcardsInQueryFilter(child, fromPaths, resultPaths));
      }
      filter.setChildren(newFilterList);
      return filter;
    }
    FunctionFilter functionFilter = (FunctionFilter) filter;
    PartialPath filterPath = functionFilter.getSinglePath();

    List<PartialPath> concatPaths = new ArrayList<>();
    if (SQLConstant.isReservedPath(filterPath)) {
      // do nothing in the case of "where time > 5"
      resultPaths.add(filterPath);
      return filter;
    } else if (filterPath.getFirstNode().startsWith(SQLConstant.ROOT)) {
      // do nothing in the case of "where root.d1.s1 > 5"
      concatPaths.add(filterPath);
    } else {
      fromPaths.forEach(fromPath -> concatPaths.add(fromPath.concatPath(filterPath)));
    }

    List<PartialPath> noStarPaths = removeWildcardsInConcatPaths(concatPaths);
    resultPaths.addAll(noStarPaths);
    if (noStarPaths.size() == 1) {
      // Transform "select s1 from root.car.* where s1 > 10" to
      // "select s1 from root.car.* where root.car.*.s1 > 10"
      functionFilter.setSinglePath(noStarPaths.get(0));
      return filter;
    } else {
      // Transform "select s1 from root.car.d1, root.car.d2 where s1 > 10" to
      // "select s1 from root.car.d1, root.car.d2 where root.car.d1.s1 > 10 and root.car.d2.s1 > 10"
      // Note that, two fork tree has to be maintained for DnfFilterOptimizer.
      return constructBinaryFilterTreeWithAnd(noStarPaths, filter);
    }
  }

  private QueryFilter constructBinaryFilterTreeWithAnd(
      List<PartialPath> noStarPaths, QueryFilter filter) throws StatementAnalyzeException {
    QueryFilter filterBinaryTree = new QueryFilter(FilterConstant.FilterType.KW_AND);
    QueryFilter currentNode = filterBinaryTree;
    for (int i = 0; i < noStarPaths.size(); i++) {
      if (i > 0 && i < noStarPaths.size() - 1) {
        QueryFilter newInnerNode = new QueryFilter(FilterConstant.FilterType.KW_AND);
        currentNode.addChildOperator(newInnerNode);
        currentNode = newInnerNode;
      }
      try {
        if (filter instanceof InFilter) {
          currentNode.addChildOperator(
              new InFilter(
                  filter.getFilterType(),
                  noStarPaths.get(i),
                  ((InFilter) filter).getNot(),
                  ((InFilter) filter).getValues()));
        } else if (filter instanceof LikeFilter) {
          currentNode.addChildOperator(
              new LikeFilter(
                  filter.getFilterType(), noStarPaths.get(i), ((LikeFilter) filter).getValue()));
        } else if (filter instanceof RegexpFilter) {
          currentNode.addChildOperator(
              new RegexpFilter(
                  filter.getFilterType(), noStarPaths.get(i), ((RegexpFilter) filter).getValue()));
        } else {
          currentNode.addChildOperator(
              new BasicFunctionFilter(
                  filter.getFilterType(),
                  noStarPaths.get(i),
                  ((BasicFunctionFilter) filter).getValue()));
        }
      } catch (SQLParserException e) {
        throw new StatementAnalyzeException(e.getMessage());
      }
    }
    return filterBinaryTree;
  }

  private List<PartialPath> removeWildcardsInConcatPaths(List<PartialPath> originalPaths)
      throws StatementAnalyzeException {
    HashSet<PartialPath> actualPaths = new HashSet<>();
    try {
      for (PartialPath originalPath : originalPaths) {
        List<MeasurementPath> all =
            schemaTree.searchMeasurementPaths(originalPath, 0, 0, false).left;
        if (all.isEmpty()) {
          throw new StatementAnalyzeException(
              String.format("Unknown time series %s in `where clause`", originalPath));
        }
        actualPaths.addAll(all);
      }
    } catch (StatementAnalyzeException e) {
      throw new StatementAnalyzeException("error when remove star: " + e.getMessage());
    }
    return new ArrayList<>(actualPaths);
  }
}
