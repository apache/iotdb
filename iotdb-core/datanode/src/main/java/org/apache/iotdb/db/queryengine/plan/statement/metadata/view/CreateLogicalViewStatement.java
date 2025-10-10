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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.view;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.exception.metadata.view.UnsupportedViewException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.analyze.SelectIntoUtils;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.IntoItem;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.view.ViewPathType;
import org.apache.iotdb.db.schemaengine.schemaregion.view.ViewPaths;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** CREATE LOGICAL VIEW statement. */
public class CreateLogicalViewStatement extends Statement {

  // the paths of this view
  private ViewPaths targetPaths;
  private IntoItem batchGenerationItem;

  // the paths of sources
  private ViewPaths sourcePaths;
  private QueryStatement queryStatement;

  // if not null, all related check and generation will be skipped
  private List<ViewExpression> viewExpressions;
  private boolean canSeeAuditDB;

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

  public List<ViewExpression> getViewExpressions() {
    return viewExpressions;
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

  public void setQueryStatement(QueryStatement queryStatement) {
    this.queryStatement = queryStatement;
  }

  /**
   * This function must be called after analyzing read statement. Expressions that analyzed should
   * be set through here.
   *
   * @param expressionList
   */
  public void setSourceExpressions(List<Expression> expressionList)
      throws UnsupportedViewException {
    // check expressions, make sure no aggregation function expression
    Pair<Boolean, UnsupportedViewException> checkResult =
        ViewPaths.checkExpressionList(expressionList);
    if (Boolean.TRUE.equals(checkResult.left)) {
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

  public void setViewExpressions(List<ViewExpression> viewExpressions) {
    this.viewExpressions = viewExpressions;
  }

  public void setTargetIntoItem(IntoItem intoItem) {
    this.targetPaths.setViewPathType(ViewPathType.BATCH_GENERATION);
    this.batchGenerationItem = intoItem;
  }

  public void parseIntoItemIfNecessary() {
    if (this.batchGenerationItem != null) {
      List<Expression> sourceExpressionList = this.getSourceExpressionList();
      IntoComponent intoComponent =
          new IntoComponent(Collections.singletonList(this.batchGenerationItem));
      intoComponent.validate(sourceExpressionList);
      IntoComponent.IntoPathIterator intoPathIterator = intoComponent.getIntoPathIterator();
      List<PartialPath> targetPathsList = new ArrayList<>();
      for (Expression sourceColumn : sourceExpressionList) {
        PartialPath deviceTemplate = intoPathIterator.getDeviceTemplate();
        String measurementTemplate = intoPathIterator.getMeasurementTemplate();

        if (sourceColumn instanceof TimeSeriesOperand) {
          PartialPath sourcePath = ((TimeSeriesOperand) sourceColumn).getPath();
          targetPathsList.add(
              SelectIntoUtils.constructTargetPath(sourcePath, deviceTemplate, measurementTemplate));
        } else {
          throw new SemanticException(
              new UnsupportedViewException(
                  "Cannot create views using data sources with calculated expressions while using into item."));
        }
      }
      this.targetPaths.setFullPathList(targetPathsList);
    }
  }

  public IntoItem getBatchGenerationItem() {
    return batchGenerationItem;
  }

  // endregion

  // region Interfaces for checking
  /**
   * Check errors in targetPaths.
   *
   * @return Pair {@literal <}Boolean, String{@literal >}. True: checks passed; False: checks
   *     failed. If check failed, return the string of illegal path.
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
   * Check errors in sourcePaths. Only usable when not using read statement. If this statement is
   * generated with a read statement, check always pass; if not, check each full paths.
   *
   * @return Pair {@literal <}Boolean, String{@literal >}. True: checks passed; False: checks
   *     failed. If check failed, return the string of illegal path.
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
   * @return return {@link true} if checks passed; else return {@link false}. If check failed,
   *     return the string of illegal path.
   */
  public Pair<Boolean, String> checkAllPaths() {
    Pair<Boolean, String> result = this.checkTargetPaths();
    if (Boolean.FALSE.equals(result.left)) {
      return result;
    }
    result = this.checkSourcePathsIfNotUsingQueryStatement();
    if (Boolean.FALSE.equals(result.left)) {
      return result;
    }
    return new Pair<>(true, null);
  }

  // endregion

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateLogicalView(this, context);
  }

  public boolean isCanSeeAuditDB() {
    return canSeeAuditDB;
  }

  public void setCanSeeAuditDB(boolean canSeeAuditDB) {
    this.canSeeAuditDB = canSeeAuditDB;
  }
}
