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

package org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl;

import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;

/**
 * Only used for meta pipe to merge the synced time series with the existing time series on receiver
 * when the tags/attributes/props are synced by snapshot.
 */
public class CreateAlignedTimeSeriesWithMergePlanImpl extends CreateAlignedTimeSeriesPlanImpl {
  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.CREATE_ALIGNED_TIME_SERIES_WITH_MERGE;
  }
}
