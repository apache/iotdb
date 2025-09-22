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
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.view.ViewPathType;
import org.apache.iotdb.db.schemaengine.schemaregion.view.ViewPaths;

import java.util.List;

public class AlterLogicalViewStatement extends Statement implements IConfigStatement {

  // the paths of this view
  private ViewPaths targetPaths;

  // the paths of sources
  private ViewPaths sourcePaths;
  private QueryStatement queryStatement;
  private boolean canSeeAuditDB;

  public AlterLogicalViewStatement() {
    super();
    this.statementType = StatementType.ALTER_LOGICAL_VIEW;
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

  public List<Expression> getSourceExpressionList() {
    this.sourcePaths.generateExpressionsIfNecessary();
    return this.sourcePaths.expressionsList;
  }

  public List<PartialPath> getTargetPathList() {
    return this.targetPaths.fullPathList;
  }

  public QueryStatement getQueryStatement() {
    return this.queryStatement;
  }

  // set source paths
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

  // set target paths
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

  public boolean isCanSeeAuditDB() {
    return canSeeAuditDB;
  }

  public void setCanSeeAuditDB(boolean canSeeAuditDB) {
    this.canSeeAuditDB = canSeeAuditDB;
  }

  // endregion

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitAlterLogicalView(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }
}
