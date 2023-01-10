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

package org.apache.iotdb.db.mpp.plan.statement.internal;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.List;
import java.util.Map;

public class SchemaFetchStatement extends Statement {

  private final PathPatternTree patternTree;
  private final Map<Integer, Template> templateMap;
  private final boolean withTags;

  public SchemaFetchStatement(
      PathPatternTree patternTree, Map<Integer, Template> templateMap, boolean withTags) {
    super();
    this.patternTree = patternTree;
    this.templateMap = templateMap;
    this.withTags = withTags;
    setType(StatementType.FETCH_SCHEMA);
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  public Map<Integer, Template> getTemplateMap() {
    return templateMap;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitSchemaFetch(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    return patternTree.getAllPathPatterns();
  }

  public boolean isWithTags() {
    return withTags;
  }
}
