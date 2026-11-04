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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.AbstractDeviceEntryMaterializer;

/** Query-scoped state shared while fetching and materializing DeviceEntry objects. */
public final class DeviceEntryFetchContext {

  private final MPPQueryContext queryContext;
  private final PlanNodeId planNodeId;
  private AbstractDeviceEntryMaterializer materializer;
  private boolean mayContainDuplicateDevice;
  private boolean containsNonAlignedDevice;

  public DeviceEntryFetchContext(final MPPQueryContext queryContext, final PlanNodeId planNodeId) {
    this.queryContext = queryContext;
    this.planNodeId = planNodeId;
  }

  public MPPQueryContext getQueryContext() {
    return queryContext;
  }

  public PlanNodeId getPlanNodeId() {
    return planNodeId;
  }

  public AbstractDeviceEntryMaterializer getMaterializer() {
    return materializer;
  }

  public void setMaterializer(final AbstractDeviceEntryMaterializer materializer) {
    this.materializer = materializer;
  }

  public boolean mayContainDuplicateDevice() {
    return mayContainDuplicateDevice;
  }

  public void setMayContainDuplicateDevice(final boolean mayContainDuplicateDevice) {
    this.mayContainDuplicateDevice = mayContainDuplicateDevice;
  }

  public boolean containsNonAlignedDevice() {
    return containsNonAlignedDevice;
  }

  public void markContainsNonAlignedDevice() {
    containsNonAlignedDevice = true;
  }
}
