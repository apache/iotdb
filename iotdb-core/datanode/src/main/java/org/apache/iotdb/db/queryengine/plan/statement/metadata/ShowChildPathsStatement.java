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
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class ShowChildPathsStatement extends ShowStatement {
  private final PartialPath partialPath;
  private PathPatternTree authorityScope = SchemaConstant.ALL_MATCH_SCOPE;

  public ShowChildPathsStatement(PartialPath partialPath) {
    super();
    this.partialPath = partialPath;
  }

  public PathPatternTree getAuthorityScope() {
    return authorityScope;
  }

  public void setAuthorityScope(PathPatternTree authorityScope) {
    this.authorityScope = authorityScope;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(partialPath);
  }

  public PartialPath getPartialPath() {
    return partialPath;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowChildPaths(this, context);
  }
}
