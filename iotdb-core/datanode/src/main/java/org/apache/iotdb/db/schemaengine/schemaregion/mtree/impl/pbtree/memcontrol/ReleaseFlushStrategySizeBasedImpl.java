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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol;

import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaEngineStatistics;

/** Threshold strategy based on the number of nodes. */
public class ReleaseFlushStrategySizeBasedImpl implements IReleaseFlushStrategy {

  private final CachedSchemaEngineStatistics engineStatistics;

  private final long releaseThreshold;
  public static final double RELEASE_THRESHOLD_RATIO = 0.70;

  public ReleaseFlushStrategySizeBasedImpl(CachedSchemaEngineStatistics engineStatistics) {
    this.engineStatistics = engineStatistics;
    long capacity =
        IoTDBDescriptor.getInstance()
            .getMemoryConfig()
            .getSchemaRegionMemoryManager()
            .getTotalMemorySizeInBytes();
    this.releaseThreshold = (long) (capacity * RELEASE_THRESHOLD_RATIO);
  }

  @Override
  public boolean isExceedReleaseThreshold() {
    return engineStatistics.getMemoryUsage() > releaseThreshold;
  }
}
