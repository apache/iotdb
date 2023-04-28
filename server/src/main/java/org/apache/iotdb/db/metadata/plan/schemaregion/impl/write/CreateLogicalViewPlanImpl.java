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
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateLogicalViewPlan;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateLogicalViewPlanImpl implements ICreateLogicalViewPlan {

  private PartialPath targetPath;
  private ViewExpression sourceExpression;

  public CreateLogicalViewPlanImpl(PartialPath targetPath, ViewExpression sourceExpression) {
    this.targetPath = targetPath;
    this.sourceExpression = sourceExpression;
  }

  @Override
  public int getViewSize() {
    return 1;
  }

  @Override
  public Map<PartialPath, ViewExpression> getViewPathToSourceExpressionMap() {
    Map<PartialPath, ViewExpression> result = new HashMap<>();
    result.put(this.targetPath, this.sourceExpression);
    return result;
  }

  @Override
  public List<PartialPath> getViewPathList() {
    return Collections.singletonList(this.targetPath);
  }

  @Override
  public void setViewPathToSourceExpressionMap(
      Map<PartialPath, ViewExpression> viewPathToSourceExpressionMap) {
    for (Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceExpressionMap.entrySet()) {
      this.targetPath = entry.getKey();
      this.sourceExpression = entry.getValue();
      break;
    }
  }

  public PartialPath getTargetPath() {
    return this.targetPath;
  }

  public ViewExpression getSourceExpression() {
    return this.sourceExpression;
  }
}
