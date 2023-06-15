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

package org.apache.iotdb.db.metadata.plan.schemaregion.impl.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.view.IAlterLogicalViewPlan;

public class AlterLogicalViewPlanImpl implements IAlterLogicalViewPlan {

  private PartialPath targetPath = null;
  private ViewExpression sourceExpression = null;

  public AlterLogicalViewPlanImpl() {}

  public AlterLogicalViewPlanImpl(PartialPath targetPath, ViewExpression sourceExpression) {
    this.targetPath = targetPath;
    this.sourceExpression = sourceExpression;
  }

  @Override
  public PartialPath getViewPath() {
    return targetPath;
  }

  public ViewExpression getSourceExpression() {
    return this.sourceExpression;
  }

  @Override
  public void setViewPath(PartialPath path) {
    this.targetPath = path;
  }

  @Override
  public void setSourceExpression(ViewExpression viewExpression) {
    this.sourceExpression = viewExpression;
  }
}
