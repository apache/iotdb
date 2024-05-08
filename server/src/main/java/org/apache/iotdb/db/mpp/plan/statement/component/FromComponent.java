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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

import java.util.ArrayList;
import java.util.List;

/** This class maintains information of {@code FROM} clause. */
public class FromComponent extends StatementNode {

  private final List<PartialPath> prefixPaths = new ArrayList<>();

  public FromComponent() {}

  public void addPrefixPath(PartialPath prefixPath) {
    prefixPaths.add(prefixPath);
  }

  public List<PartialPath> getPrefixPaths() {
    return prefixPaths;
  }

  public String toSQLString() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("FROM").append(' ');
    for (int i = 0; i < prefixPaths.size(); i++) {
      sqlBuilder.append(prefixPaths.get(i).toString());
      if (i < prefixPaths.size() - 1) {
        sqlBuilder.append(", ");
      }
    }
    return sqlBuilder.toString();
  }
}
