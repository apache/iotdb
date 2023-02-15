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

import java.util.concurrent.atomic.AtomicLong;

public class ReleaseFlushStrategyNumBasedImpl implements IReleaseFlushStrategy {

  private final int capacity;

  private final AtomicLong unpinnedNum;
  private final AtomicLong pinnedNum;

  public ReleaseFlushStrategyNumBasedImpl(AtomicLong unpinnedNum, AtomicLong pinnedNum) {
    this.unpinnedNum = unpinnedNum;
    this.pinnedNum = pinnedNum;
    this.capacity = IoTDBDescriptor.getInstance().getConfig().getCachedMNodeSizeInSchemaFileMode();
  }

  @Override
  public boolean isExceedReleaseThreshold() {
    return pinnedNum.get() + unpinnedNum.get() > capacity * 0.6;
  }

  @Override
  public boolean isExceedFlushThreshold() {
    return pinnedNum.get() + unpinnedNum.get() > capacity;
  }
}
