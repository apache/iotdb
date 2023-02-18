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

public interface IPreDeleteTimeSeriesPlan extends ISchemaRegionPlan {

  @Override
  default SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.PRE_DELETE_TIMESERIES_IN_CLUSTER;
  }

  @Override
  default <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitPreDeleteTimeSeries(this, context);
  }

  PartialPath getPath();

  void setPath(PartialPath path);
}
