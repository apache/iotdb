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

package org.apache.iotdb.db.metadata.view;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;

import java.util.ArrayList;
import java.util.List;

/**
 * A class to save all paths' info in targetPaths and sourcePaths except query statement.
 *
 * <p>fullPathList: CREATE VIEW root.db.device.temp AS root.ln.d.s01 PathGroup: CREATE VIEW
 * root.db(device.temp, status) AS root.ln(d.s01, wf.abc.s02)
 */
public class ViewPaths {
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
