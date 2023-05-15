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

package org.apache.iotdb.db.metadata.plan.schemaregion.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;

import java.util.List;
import java.util.Map;

public interface ICreateLogicalViewPlan extends ISchemaRegionPlan {
  @Override
  default SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.CREATE_LOGICAL_VIEW;
  }

  @Override
  default <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitCreateLogicalView(this, context);
  }

  /**
   * Get how many views will be created.
   *
   * @return the number of views that will be created
   */
  int getViewSize();

  /** @return return a map, from view path (the targets) to source expression. */
  Map<PartialPath, ViewExpression> getViewPathToSourceExpressionMap();

  /** @return return a list of target paths (paths of views). */
  List<PartialPath> getViewPathList();

  /**
   * @param viewPathToSourceExpressionMap a map from view path (the targets) to source expression.
   */
  void setViewPathToSourceExpressionMap(
      Map<PartialPath, ViewExpression> viewPathToSourceExpressionMap);

  //  /**
  //   * @param partialPaths a list of partialPaths. Will be transformed into expressions then set
  // as source. (source timeseries or expressions in query statement)
  //   */
  //  void setSourceByPartialPath(List<PartialPath> partialPaths);
}
