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

package org.apache.iotdb.db.mpp.plan.statement.metadata.view;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.exception.metadata.view.UnsupportedViewException;
import org.apache.iotdb.db.metadata.view.ViewPathType;
import org.apache.iotdb.db.metadata.view.ViewPaths;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

/** CREATE LOGICAL VIEW statement. */
public class CreateLogicalViewStatement extends Statement {

  // the paths of this view
  private ViewPaths targetPaths;

  // the paths of sources
  private ViewPaths sourcePaths;
  private QueryStatement queryStatement;

  // if not null, all related check and generation will be skipped
  private ViewExpression viewExpression;

  public CreateLogicalViewStatement() {
    super();
    this.statementType = StatementType.CREATE_LOGICAL_VIEW;
    this.sourcePaths = new ViewPaths();
    this.targetPaths = new ViewPaths();
  }

  // region Interfaces about setting and getting

  // get paths
  @Override
  public List<PartialPath> getPaths() {
    return this.getTargetPathList();
  }

  public ViewPaths getTargetPaths() {
    return targetPaths;
  }

  public ViewPaths getSourcePaths() {
    return sourcePaths;
  }

  public List<PartialPath> getTargetPathList() {
    return this.targetPaths.fullPathList;
  }

  public List<Expression> getSourceExpressionList() {
    this.sourcePaths.generateExpressionsIfNecessary();
    return this.sourcePaths.expressionsList;
  }

  public QueryStatement getQueryStatement() {
    return this.queryStatement;
  }

  public ViewExpression getViewExpression() {
    return viewExpression;
  }

  // set source paths
  public void setSourcePaths(ViewPaths sourcePaths) {
    this.sourcePaths = sourcePaths;
  }

  public void setSourceFullPaths(List<PartialPath> paths) {
    this.sourcePaths.setViewPathType(ViewPathType.FULL_PATH_LIST);
    this.sourcePaths.setFullPathList(paths);
  }

  public void setSourcePathsGroup(PartialPath prefixPath, List<PartialPath> suffixPaths) {
    this.sourcePaths.setViewPathType(ViewPathType.PATHS_GROUP);
    this.sourcePaths.setPrefixOfPathsGroup(prefixPath);
    this.sourcePaths.setSuffixOfPathsGroup(suffixPaths);
    this.sourcePaths.generateFullPathsFromPathsGroup();
  }

  public void setSourceQueryStatement(QueryStatement queryStatement) {
    this.sourcePaths.setViewPathType(ViewPathType.QUERY_STATEMENT);
    this.queryStatement = queryStatement;
  }

  /**
   * This function must be called after analyzing query statement. Expressions that analyzed should
   * be set through here.
   *
   * @param expressionList
   */
  public void setSourceExpressions(List<Expression> expressionList)
      throws UnsupportedViewException {
    // check expressions, make sure no aggregation function expression
    Pair<Boolean, UnsupportedViewException> checkResult =
        ViewPaths.checkExpressionList(expressionList);
    if (checkResult.left) {
      this.sourcePaths.setExpressionsList(expressionList);
    } else {
      throw checkResult.right;
    }
  }

  // set target paths
  public void setTargetPaths(ViewPaths targetPaths) {
    this.targetPaths = targetPaths;
  }

  public void setTargetFullPaths(List<PartialPath> paths) {
    this.targetPaths.setViewPathType(ViewPathType.FULL_PATH_LIST);
    this.targetPaths.setFullPathList(paths);
  }

  public void setTargetPathsGroup(PartialPath prefixPath, List<PartialPath> suffixPaths) {
    this.targetPaths.setViewPathType(ViewPathType.PATHS_GROUP);
    this.targetPaths.setPrefixOfPathsGroup(prefixPath);
    this.targetPaths.setSuffixOfPathsGroup(suffixPaths);
    this.targetPaths.generateFullPathsFromPathsGroup();
  }

  public void setViewExpression(ViewExpression viewExpression) {
    this.viewExpression = viewExpression;
  }

  // endregion

  // region Interfaces for checking
  /**
   * Check errors in targetPaths.
   *
   * @return Pair<Boolean, String>. True: checks passed; False: checks failed. if check failed,
   *     return the string of illegal path.
   */
  public Pair<Boolean, String> checkTargetPaths() {
    for (PartialPath thisPath : this.getTargetPathList()) {
      if (thisPath.getNodeLength() < 3) {
        return new Pair<>(false, thisPath.getFullPath());
      }
    }
    return new Pair<>(true, null);
  }

  /**
   * Check errors in sourcePaths. Only usable when not using query statement. If this statement is
   * generated with a query statement, check always pass; if not, check each full paths.
   *
   * @return Pair<Boolean, String>. True: checks passed; False: checks failed. if check failed,
   *     return the string of illegal path.
   */
  public Pair<Boolean, String> checkSourcePathsIfNotUsingQueryStatement() {
    if (this.sourcePaths.viewPathType == ViewPathType.PATHS_GROUP
        || this.sourcePaths.viewPathType == ViewPathType.FULL_PATH_LIST) {
      for (PartialPath thisPath : this.sourcePaths.fullPathList) {
        if (thisPath.getNodeLength() < 3) {
          return new Pair<>(false, thisPath.getFullPath());
        }
      }
    }
    return new Pair<>(true, null);
  }

  /**
   * @return return true if checks passed; else return false. if check failed, return the string of
   *     illegal path.
   */
  public Pair<Boolean, String> checkAllPaths() {
    Pair<Boolean, String> result = this.checkTargetPaths();
    if (result.left == false) return result;
    result = this.checkSourcePathsIfNotUsingQueryStatement();
    if (result.left == false) return result;
    return new Pair<>(true, null);
  }

  // endregion

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateLogicalView(this, context);
  }
}
