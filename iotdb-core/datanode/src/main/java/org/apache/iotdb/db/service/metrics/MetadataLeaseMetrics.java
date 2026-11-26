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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

/**
 * Exposes the DataNode's metadata-lease state for observability: how long it has been since the
 * last ConfigNode heartbeat was received. A value approaching {@code metadata_lease_fence_ms}
 * indicates the DataNode is about to (or has) self-fenced its ConfigNode-pushed metadata caches.
 */
public class MetadataLeaseMetrics implements IMetricSet {

  private static final String METADATA_LEASE_HEARTBEAT_AGE_MS = "metadata_lease_heartbeat_age_ms";

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    metricService.createAutoGauge(
        METADATA_LEASE_HEARTBEAT_AGE_MS,
        MetricLevel.IMPORTANT,
        MetadataLeaseManager.getInstance(),
        MetadataLeaseManager::getMillisSinceLastConfigNodeHeartbeat);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    metricService.remove(MetricType.AUTO_GAUGE, METADATA_LEASE_HEARTBEAT_AGE_MS);
  }
}
