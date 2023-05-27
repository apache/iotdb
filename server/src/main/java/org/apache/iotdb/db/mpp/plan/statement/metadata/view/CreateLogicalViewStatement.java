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
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;

/** CREATE LOGICAL VIEW statement. */
public class CreateLogicalViewStatement extends Statement {

  // the paths of this view
  private viewPaths targetPaths;

  // the paths of sources
  private viewPaths sourcePaths;
  private QueryStatement queryStatement;

  // if not null, all related check and generation will be skipped
  private ViewExpression viewExpression;

  public CreateLogicalViewStatement() {
    super();
    this.statementType = StatementType.CREATE_LOGICAL_VIEW;
    this.sourcePaths = new viewPaths();
    this.targetPaths = new viewPaths();
  }

  // region Interfaces about setting and getting

  // get paths
  @Override
  public List<PartialPath> getPaths() {
    return this.getTargetPathList();
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
  public void setSourceExpressions(List<Expression> expressionList) {
    this.sourcePaths.setExpressionsList(expressionList);
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

  // region private classes

  private enum ViewPathType {
    FULL_PATH_LIST,
    PATHS_GROUP,
    QUERY_STATEMENT
  }

  /**
   * A private class to save all paths' info in targetPaths and sourcePaths except query statement.
   *
   * <p>fullPathList: CREATE VIEW root.db.device.temp AS root.ln.d.s01 PathGroup: CREATE VIEW
   * root.db(device.temp, status) AS root.ln(d.s01, wf.abc.s02)
   */
  private class viewPaths {
    public ViewPathType viewPathType = ViewPathType.FULL_PATH_LIST;
    public List<PartialPath> fullPathList = null;
    public PartialPath prefixOfPathsGroup = null;
    public List<PartialPath> suffixOfPathsGroup = null;

    public List<Expression> expressionsList = null;

    public void addPath(PartialPath path) {
      if (this.fullPathList == null) {
        this.fullPathList = new ArrayList<>();
        this.fullPathList.add(path);
      } else {
        this.fullPathList.add(path);
      }
    }

    public void setFullPathList(List<PartialPath> pathList) {
      this.fullPathList = pathList;
    }

    public void setPrefixOfPathsGroup(PartialPath path) {
      this.prefixOfPathsGroup = path;
    }

    public void setSuffixOfPathsGroup(List<PartialPath> pathList) {
      this.suffixOfPathsGroup = pathList;
    }

    public void setViewPathType(ViewPathType viewPathType) {
      this.viewPathType = viewPathType;
    }

    public void generateFullPathsFromPathsGroup() {
      if (prefixOfPathsGroup != null && suffixOfPathsGroup != null) {
        this.fullPathList = new ArrayList<>();
        for (PartialPath suffixPath : suffixOfPathsGroup) {
          PartialPath pathToAdd = prefixOfPathsGroup.concatPath(suffixPath);
          this.addPath(pathToAdd);
        }
      }
    }

    public void generateExpressionsIfNecessary() {
      if (this.viewPathType == ViewPathType.FULL_PATH_LIST
          || this.viewPathType == ViewPathType.PATHS_GROUP) {
        if (this.fullPathList != null) {
          this.expressionsList = new ArrayList<>();
          for (PartialPath path : this.fullPathList) {
            TimeSeriesOperand tsExpression = new TimeSeriesOperand(path);
            this.expressionsList.add(tsExpression);
          }
        }
      } else if (this.viewPathType == ViewPathType.QUERY_STATEMENT) {
        // no nothing. expressions should be set by setExpressionsList
      }
    }

    public void setExpressionsList(List<Expression> expressionsList) {
      this.expressionsList = expressionsList;
    }
    // end of viewPaths
  }

  // endregion
}
