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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.template;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class ActivateTemplateStatement extends Statement {

  private PartialPath path;

  public ActivateTemplateStatement() {
    super();
    statementType = StatementType.ACTIVATE_TEMPLATE;
  }

  public ActivateTemplateStatement(PartialPath path) {
    super();
    this.path = path;
    statementType = StatementType.ACTIVATE_TEMPLATE;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(path);
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitActivateTemplate(this, context);
  }
}
