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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.ArrayList;
import java.util.List;

public class ShowTTLStatement extends ShowStatement implements IConfigStatement {
  private List<PartialPath> pathPatterns = new ArrayList<>();
  private PathPatternTree authorityScope = SchemaConstant.ALL_MATCH_SCOPE;
  private boolean isAll = false;

  public boolean isAll() {
    return isAll;
  }

  public PathPatternTree getAuthorityScope() {
    return authorityScope;
  }

  public void setAuthorityScope(PathPatternTree authorityScope) {
    this.authorityScope = authorityScope;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowTTL(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public List<PartialPath> getPaths() {
    return pathPatterns;
  }

  public void addPathPatterns(PartialPath pathPattern) {
    pathPatterns.add(pathPattern);
  }

  public void setAll(boolean all) {
    isAll = all;
  }
}
