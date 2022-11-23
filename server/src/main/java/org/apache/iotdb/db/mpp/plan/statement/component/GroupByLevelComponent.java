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

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

import java.util.ArrayList;
import java.util.List;

/** This class maintains information of {@code GROUP BY LEVEL} clause. */
public class GroupByLevelComponent extends StatementNode {

  protected int[] levels;
  protected List<Boolean> isCountStar = new ArrayList<>();

  public int[] getLevels() {
    return levels;
  }

  public void setLevels(int[] levels) {
    this.levels = levels;
  }

  public void updateIsCountStar(Expression rawExpression) {
    if (rawExpression instanceof FunctionExpression) {
      isCountStar.add(((FunctionExpression) rawExpression).isCountStar());
    } else {
      isCountStar.add(false);
    }
  }

  public boolean isCountStar(int i) {
    return isCountStar.get(i);
  }

  public String toSQLString(boolean hasGroupByTime) {
    StringBuilder sqlBuilder = new StringBuilder();
    if (hasGroupByTime) {
      sqlBuilder.append(", ");
    } else {
      sqlBuilder.append("GROUP BY ");
    }
    sqlBuilder.append("LEVEL = ");
    for (int i = 0; i < levels.length; i++) {
      sqlBuilder.append(levels[i]);
      if (i < levels.length - 1) {
        sqlBuilder.append(',').append(' ');
      }
    }
    return sqlBuilder.toString();
  }
}
