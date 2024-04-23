/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.tsfile.file.metadata.IDeviceID;

public class PartitionLastFlushTime implements ILastFlushTime {

  private long partitionLastFlushTime;

  public PartitionLastFlushTime(long initPartitionLastFlushTime) {
    this.partitionLastFlushTime = initPartitionLastFlushTime;
  }

  @Override
  public long getLastFlushTime(IDeviceID device) {
    return partitionLastFlushTime;
  }

  @Override
  public void updateLastFlushTime(IDeviceID device, long time) {
    partitionLastFlushTime = Math.max(partitionLastFlushTime, time);
  }

  @Override
  public ILastFlushTime degradeLastFlushTime() {
    return this;
  }
}
