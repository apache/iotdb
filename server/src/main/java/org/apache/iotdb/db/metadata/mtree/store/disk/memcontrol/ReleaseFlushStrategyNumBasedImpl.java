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
package org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaEngineStatistics;

/** Number-based threshold strategy */
public class ReleaseFlushStrategyNumBasedImpl implements IReleaseFlushStrategy {

  private final int capacity;

  private final CachedSchemaEngineStatistics engineStatistics;

  public ReleaseFlushStrategyNumBasedImpl(CachedSchemaEngineStatistics engineStatistics) {
    this.engineStatistics = engineStatistics;
    this.capacity = IoTDBDescriptor.getInstance().getConfig().getCachedMNodeSizeInSchemaFileMode();
  }

  @Override
  public boolean isExceedReleaseThreshold() {
    return engineStatistics.getPinnedMNodeNum() + engineStatistics.getUnpinnedMNodeNum()
        > capacity * 0.6;
  }

  @Override
  public boolean isExceedFlushThreshold() {
    return engineStatistics.getPinnedMNodeNum() + engineStatistics.getUnpinnedMNodeNum() > capacity;
  }
}
