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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import java.util.Collections;
import java.util.List;

public final class InMemoryDeviceEntrySource implements BatchDeviceEntrySource {

  private List<DeviceEntry> entries;

  public InMemoryDeviceEntrySource(List<DeviceEntry> entries) {
    this.entries = entries;
  }

  @Override
  public boolean hasNextBatch() {
    return entries != null;
  }

  @Override
  public List<DeviceEntry> nextBatch() {
    if (entries == null) {
      return Collections.emptyList();
    }
    List<DeviceEntry> result = entries;
    entries = null;
    return result;
  }

  @Override
  public void close() {
    entries = null;
  }
}
