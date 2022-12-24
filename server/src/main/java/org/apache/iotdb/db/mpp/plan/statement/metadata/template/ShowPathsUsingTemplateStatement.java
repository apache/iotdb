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

package org.apache.iotdb.db.mpp.plan.statement.metadata.template;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStatement;

import java.util.Collections;
import java.util.List;

public class ShowPathsUsingTemplateStatement extends ShowStatement {

  private PartialPath pathPattern;

  private String templateName;

  public ShowPathsUsingTemplateStatement(PartialPath pathPattern, String templateName) {
    super();
    statementType = StatementType.SHOW_PATH_USING_SCHEMA_TEMPLATE;
    this.pathPattern = pathPattern;
    this.templateName = templateName;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public void setPathPattern(PartialPath pathPattern) {
    this.pathPattern = pathPattern;
  }

  public String getTemplateName() {
    return templateName;
  }

  public void setTemplateName(String templateName) {
    this.templateName = templateName;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowPathsUsingTemplate(this, context);
  }
}
